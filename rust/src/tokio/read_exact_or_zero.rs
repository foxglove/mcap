use tokio::io::{AsyncRead, AsyncReadExt};

/// read up to buf.len() bytes from  `r` into `buf`. This repeatedly calls read() on `r` until
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
