use std::io::{Error, ErrorKind, Result};
use std::pin::{pin, Pin};
use std::ptr;
use std::task::{Context, Poll};

use lz4::liblz4::{
    check_error, LZ4FDecompressionContext, LZ4F_createDecompressionContext, LZ4F_decompress,
    LZ4F_freeDecompressionContext, LZ4F_VERSION,
};
use tokio::io::{AsyncRead, ReadBuf};

const BUFFER_SIZE: usize = 32 * 1024;

#[derive(Debug)]
struct DecoderContext {
    c: LZ4FDecompressionContext,
}

// An adaptation of the [`lz4::Decoder`] [`std::io::Read`] impl, but for [`tokio::io::AsyncRead`].
// Code below is adapted from the [lz4](https://github.com/10XGenomics/lz4-rs) crate source.
#[derive(Debug)]
pub struct Lz4Decoder<R> {
    c: DecoderContext,
    r: R,
    input_buf: Box<[u8]>,
    unread_input_start: usize,
    unread_input_end: usize,
    next: usize,
}

impl<R> Lz4Decoder<R> {
    /// Creates a new decoder which reads its input from the given
    /// input stream. The input stream can be re-acquired by calling
    /// `finish()`
    pub fn new(r: R) -> Result<Lz4Decoder<R>> {
        Ok(Lz4Decoder {
            r,
            c: DecoderContext::new()?,
            input_buf: vec![0; BUFFER_SIZE].into_boxed_slice(),
            unread_input_start: BUFFER_SIZE,
            unread_input_end: BUFFER_SIZE,
            // Minimal LZ4 stream size
            next: 11,
        })
    }

    pub fn finish(self) -> (R, Result<()>) {
        (
            self.r,
            match self.next {
                0 => Ok(()),
                _ => Err(Error::new(
                    ErrorKind::Interrupted,
                    "Finish called before end of compressed stream",
                )),
            },
        )
    }
}

impl<R: AsyncRead + std::marker::Unpin> AsyncRead for Lz4Decoder<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output_buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Thre's nothing left to read.
        if self.next == 0 || output_buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut written_len: usize = 0;
        let this = self.get_mut();
        while written_len == 0 {
            // this reader buffers input data until it has enough to present to the lz4 frame decoder.
            // if there's nothing unread, request more data from the reader.
            if this.unread_input_start >= this.unread_input_end {
                // request a full BUFFER_SIZE or the amount requested by the lz4 frame decoder,
                // whichever is less.
                let need = std::cmp::min(BUFFER_SIZE, this.next);
                // try reading more input data. If it's not ready, return and try again later.
                // NOTE: we don't need to save this stack frame as a future and re-enter it later
                // because the only frame-local state `written_len` has not been modified and can be
                // discarded.
                {
                    let mut input_buf = ReadBuf::new(&mut this.input_buf[..need]);
                    let result = pin!(&mut this.r).poll_read(cx, &mut input_buf);
                    match result {
                        Poll::Pending => return result,
                        Poll::Ready(Err(_)) => return result,
                        _ => {}
                    };
                    this.unread_input_start = 0;
                    this.unread_input_end = input_buf.filled().len();
                    this.next -= this.unread_input_end;
                }
                // The read succeeded. If zero bytes were read, we're at the end of the stream.
                if this.unread_input_end == 0 {
                    return Poll::Ready(Ok(()));
                }
            }
            // feed bytes from our input buffer into the compressor, writing into the output
            // buffer until either the output buffer is full or the input buffer is consumed.
            while (written_len < output_buf.remaining())
                && (this.unread_input_start < this.unread_input_end)
            {
                let mut src_size = this.unread_input_end - this.unread_input_start;
                let mut dst_size = output_buf.remaining() - written_len;
                let prev_filled = output_buf.filled().len();
                let len = check_error(unsafe {
                    LZ4F_decompress(
                        this.c.c,
                        output_buf.initialize_unfilled().as_mut_ptr(),
                        &mut dst_size,
                        this.input_buf[this.unread_input_start..].as_ptr(),
                        &mut src_size,
                        ptr::null(),
                    )
                })?;
                this.unread_input_start += src_size;
                written_len += dst_size;
                output_buf.set_filled(prev_filled + written_len);
                if len == 0 {
                    this.next = 0;
                    return Poll::Ready(Ok(()));
                } else if this.next < len {
                    this.next = len;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl DecoderContext {
    fn new() -> Result<DecoderContext> {
        let mut context = LZ4FDecompressionContext(ptr::null_mut());
        check_error(unsafe { LZ4F_createDecompressionContext(&mut context, LZ4F_VERSION) })?;
        Ok(DecoderContext { c: context })
    }
}

impl Drop for DecoderContext {
    fn drop(&mut self) {
        unsafe { LZ4F_freeDecompressionContext(self.c) };
    }
}
