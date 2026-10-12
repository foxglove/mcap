//! Implement decompression algorithms for MCAP chunk data
use crate::{McapError, McapResult};

/// How many bytes one [`Decompressor::decompress`] call consumed and wrote.
#[derive(Debug, Clone, Copy)]
pub struct DecompressResult {
    /// The number of bytes consumed from the input buffer.
    pub consumed: usize,
    /// The number of bytes written to the output buffer.
    pub wrote: usize,
}

/// A streaming decompressor for one MCAP chunk compression format.
///
/// [`name`](Self::name) is the chunk `compression` string this decompressor handles. Register an
/// instance before the reader reaches a chunk with that name.
/// [`LinearReader`](crate::sans_io::LinearReader),
/// [`IndexedReader`](crate::sans_io::IndexedReader), and the readers that wrap `LinearReader`
/// accept one. The readers in [`crate::read`] do not.
///
/// The built-in `"zstd"` and `"lz4"` decoders are registered by default when their crate
/// features are enabled. Registering another decompressor under the same name replaces them.
///
/// One instance is kept per name. [`reset`](Self::reset) is called after each chunk, and not
/// before the first, so the instance must be ready to decode when it is registered.
pub trait Decompressor: Send {
    /// How many compressed bytes should be available in `src` on the next
    /// [`decompress`](Self::decompress) call.
    ///
    /// Return 0 when any amount of input is acceptable. Otherwise return how many bytes to buffer
    /// before the next call, not a minimum. A caller may pass fewer bytes than this asks for.
    fn next_read_size(&self) -> usize;
    /// Decompresses bytes from `src` into `dst`.
    ///
    /// Consume at most `src.len()` bytes and write at most `dst.len()` bytes. [`DecompressResult`]
    /// reports those counts and must not claim more. `dst` may be as small as one byte; keep
    /// decoded output that does not fit and write it on a later call.
    ///
    /// A chunk may hold several frames, including skippable frames. Continue across frame
    /// boundaries until the caller stops. The caller stops once the chunk's declared uncompressed
    /// size has been written, and does not pass compressed bytes left after that point.
    ///
    /// Return `consumed: 0` and `wrote: 0` only when no output can be produced without more
    /// compressed input. Returning that when `src` already holds enough input to make progress is
    /// an error.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Prepares this instance to decode another chunk.
    ///
    /// Called after each chunk, not before the first. On failure the error is returned and the
    /// instance stays registered.
    fn reset(&mut self) -> McapResult<()>;
    /// The chunk `compression` string this decompressor handles.
    ///
    /// Must be non-empty. An empty string means the chunk is not compressed, and registering this
    /// decompressor fails with [`McapError::EmptyDecompressorName`].
    fn name(&self) -> &'static str;
}

/// A reader's decompressors, looked up by [`Decompressor::name`].
///
/// A reader holds a handful, so scanning their names is cheaper than hashing the chunk's
/// compression string.
pub(crate) struct Decompressors(Vec<Box<dyn Decompressor>>);

impl Decompressors {
    /// The built-in decoders enabled by crate features. Each allocates its native context on
    /// first use, so this is cheap for readers that never meet a compressed chunk.
    pub(crate) fn builtin() -> Self {
        Self(vec![
            #[cfg(feature = "zstd")]
            Box::new(crate::sans_io::zstd::ZstdDecoder::new()),
            #[cfg(feature = "lz4")]
            Box::new(crate::sans_io::lz4::Lz4Decoder::new()),
        ])
    }

    /// Stores `decompressor`, replacing any decompressor with the same name.
    pub(crate) fn register(&mut self, decompressor: impl Decompressor + 'static) -> McapResult<()> {
        let name = decompressor.name();
        if name.is_empty() {
            return Err(McapError::EmptyDecompressorName);
        }
        match self.position(name) {
            Some(i) => self.0[i] = Box::new(decompressor),
            None => self.0.push(Box::new(decompressor)),
        }
        Ok(())
    }

    /// Decodes a chunk that is fully in memory into `dst`, then resets the decompressor.
    ///
    /// A `decompress` call that makes no progress is an error, since no more input can arrive.
    /// Input past the point where `dst` is full is not passed on. `reset` runs after a
    /// decompression error too, and the decompression error wins.
    pub(crate) fn decompress_chunk(
        &mut self,
        name: &str,
        src: &[u8],
        dst: &mut Vec<u8>,
        uncompressed_size: usize,
    ) -> McapResult<()> {
        let decompressor = match self.position(name) {
            Some(i) => self.0[i].as_mut(),
            None => return Err(McapError::UnsupportedCompression(name.into())),
        };
        dst.clear();
        dst.resize(uncompressed_size, 0);
        let mut input_pos = 0;
        let mut output_pos = 0;
        let decompressed = (|| {
            while output_pos < uncompressed_size {
                let input = &src[input_pos..];
                let output = &mut dst[output_pos..uncompressed_size];
                let output_len = output.len();
                let res = check_decompress_result(
                    decompressor.decompress(input, output)?,
                    input.len(),
                    output_len,
                )?;
                if res.consumed == 0 && res.wrote == 0 {
                    return Err(no_progress_error());
                }
                input_pos += res.consumed;
                output_pos += res.wrote;
            }
            Ok(())
        })();
        decompressed.and(decompressor.reset())
    }

    /// Removes the decompressor named `name`, for a reader that streams a chunk across calls.
    pub(crate) fn take(&mut self, name: &str) -> Option<Box<dyn Decompressor>> {
        let i = self.position(name)?;
        Some(self.0.swap_remove(i))
    }

    /// Resets a decompressor from [`take`](Self::take) and puts it back. If a replacement was
    /// registered under its name in the meantime, the replacement stays and `decompressor` is
    /// dropped without a reset. The instance goes back even when reset fails, so the next chunk
    /// reports the failure instead of finding no decompressor.
    pub(crate) fn restore(&mut self, mut decompressor: Box<dyn Decompressor>) -> McapResult<()> {
        if self.position(decompressor.name()).is_some() {
            return Ok(());
        }
        let reset = decompressor.reset();
        self.0.push(decompressor);
        reset
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.position(name).is_some()
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.0
            .iter()
            .position(|decompressor| decompressor.name() == name)
    }
}

/// Rejects a [`DecompressResult`] that reports more bytes than the buffers it was given.
pub(crate) fn check_decompress_result(
    result: DecompressResult,
    src_len: usize,
    dst_len: usize,
) -> McapResult<DecompressResult> {
    if result.consumed > src_len || result.wrote > dst_len {
        return Err(McapError::DecompressionError(
            "decompressor reported more bytes than the buffers provided".into(),
        ));
    }
    Ok(result)
}

pub(crate) fn no_progress_error() -> McapError {
    McapError::DecompressionError("decompressor made no progress".into())
}
