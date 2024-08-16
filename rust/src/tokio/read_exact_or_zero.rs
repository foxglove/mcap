use tokio::io::{AsyncRead, AsyncReadExt};

/// read from `r` into `buf` until `buf` is completely full or EOF is reached.
/// if R was already at EOF, this is not considered an error.
/// If R was not at EOF, but EOF came before the end of the buffer, this is considered an error.
/// This is useful for cases where we expect either another record full record or EOF.
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
