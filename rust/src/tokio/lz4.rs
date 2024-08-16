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

// An equivalent of the lz4::Decoder `std::io::Read` wrapper for `tokio::io::AsyncRead`.
// Code below is adapted from the https://github.com/bozaro/lz4-rs crate.
#[derive(Debug)]
pub struct Lz4Decoder<R> {
    c: DecoderContext,
    r: R,
    buf: Box<[u8]>,
    pos: usize,
    len: usize,
    next: usize,
}

impl<R: AsyncRead> Lz4Decoder<R> {
    /// Creates a new decoder which reads its input from the given
    /// input stream. The input stream can be re-acquired by calling
    /// `finish()`
    pub fn new(r: R) -> Result<Lz4Decoder<R>> {
        Ok(Lz4Decoder {
            r,
            c: DecoderContext::new()?,
            buf: vec![0; BUFFER_SIZE].into_boxed_slice(),
            pos: BUFFER_SIZE,
            len: BUFFER_SIZE,
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
                    "Finish runned before read end of compressed stream",
                )),
            },
        )
    }
}

impl<R: AsyncRead + std::marker::Unpin> AsyncRead for Lz4Decoder<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.next == 0 || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut written_len: usize = 0;
        let mself = self.get_mut();
        while written_len == 0 {
            if mself.pos >= mself.len {
                let need = if mself.buf.len() < mself.next {
                    mself.buf.len()
                } else {
                    mself.next
                };
                {
                    let mut comp_buf = ReadBuf::new(&mut mself.buf[..need]);
                    let result = pin!(&mut mself.r).poll_read(cx, &mut comp_buf);
                    match result {
                        Poll::Pending => return result,
                        Poll::Ready(Err(_)) => return result,
                        _ => {}
                    };
                    mself.len = comp_buf.filled().len();
                }
                if mself.len == 0 {
                    break;
                }
                mself.pos = 0;
                mself.next -= mself.len;
            }
            while (written_len < buf.remaining()) && (mself.pos < mself.len) {
                let mut src_size = mself.len - mself.pos;
                let mut dst_size = buf.remaining() - written_len;
                let prev_filled = buf.filled().len();
                let len = check_error(unsafe {
                    LZ4F_decompress(
                        mself.c.c,
                        buf.initialize_unfilled().as_mut_ptr(),
                        &mut dst_size,
                        mself.buf[mself.pos..].as_ptr(),
                        &mut src_size,
                        ptr::null(),
                    )
                })?;
                mself.pos += src_size;
                written_len += dst_size;
                buf.set_filled(prev_filled + written_len);
                if len == 0 {
                    mself.next = 0;
                    return Poll::Ready(Ok(()));
                } else if mself.next < len {
                    mself.next = len;
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
