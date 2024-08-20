use tokio::io::{AsyncRead, AsyncReadExt};

/// read up to `buf.len()` bytes from  `r` into `buf`. This repeatedly calls read() on `r` until
/// either the buffer is full or EOF is reached. If either 0 or buf.len() bytes were read before
/// EOF, Ok(n) is returned. If EOF is reached after 0 bytes but before buf.len(), Err(UnexpectedEOF)
/// is returned.
/// This is useful for cases where we expect either to read either a whole MCAP record or EOF.
pub(crate) async fn read_exact_or_zero<R: AsyncRead + std::marker::Unpin>(
    r: &mut R,
    buf: &mut [u8],
) -> Result<usize, std::io::Error> {
    let mut pos: usize = 0;
    loop {
        let readlen = r.read(&mut buf[pos..]).await?;
        if readlen == 0 {
            if pos != 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            } else {
                return Ok(0);
            }
        }
        pos += readlen;
        if pos == buf.len() {
            return Ok(pos);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cmp::min;

    struct ZeroReader {
        remaining: usize,
        max_read_len: usize,
    }

    impl AsyncRead for ZeroReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let max_read_len = self.as_ref().max_read_len;
            let remaining = self.as_ref().remaining;
            if remaining == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            let to_fill = min(min(remaining, buf.remaining()), max_read_len);
            buf.initialize_unfilled_to(to_fill).fill(0);
            buf.set_filled(to_fill);
            self.as_mut().remaining -= to_fill;
            return std::task::Poll::Ready(Ok(()));
        }
    }
    #[tokio::test]
    async fn test_full_read_is_not_error() {
        let mut r = ZeroReader {
            remaining: 10,
            max_read_len: 10,
        };
        let mut buf: Vec<u8> = vec![1; 10];
        let result = read_exact_or_zero(&mut r, &mut buf).await;
        assert_eq!(result.ok(), Some(10));
        assert_eq!(&buf[..], &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    }

    #[tokio::test]
    async fn test_eof_is_not_error() {
        let mut r = ZeroReader {
            remaining: 0,
            max_read_len: 10,
        };
        let mut buf: Vec<u8> = vec![1; 10];
        let result = read_exact_or_zero(&mut r, &mut buf).await;
        assert_eq!(result.ok(), Some(0));
        assert_eq!(&buf[..], &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    }
    #[tokio::test]
    async fn test_repeated_read_calls() {
        let mut r = ZeroReader {
            remaining: 10,
            max_read_len: 1,
        };
        let mut buf: Vec<u8> = vec![1; 10];
        let result = read_exact_or_zero(&mut r, &mut buf).await;
        assert_eq!(result.ok(), Some(10));
        assert_eq!(&buf[..], &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    }
    #[tokio::test]
    async fn test_partial_read_is_error() {
        let mut r = ZeroReader {
            remaining: 4,
            max_read_len: 2,
        };
        let mut buf: Vec<u8> = vec![1; 10];
        let result = read_exact_or_zero(&mut r, &mut buf).await;
        assert!(!result.is_ok());
        assert_eq!(&buf[..], &[0, 0, 0, 0, 1, 1, 1, 1, 1, 1]);
    }
}
