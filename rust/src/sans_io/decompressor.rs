use crate::McapResult;
pub struct DecompressResult {
    pub consumed: usize,
    pub wrote: usize,
    pub need: usize,
}

pub trait Decompressor {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    fn reset(&mut self) -> McapResult<()>;
    fn name(&self) -> &'static str;
}

pub struct NoneDecompressor {}

impl Decompressor for NoneDecompressor {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let len = std::cmp::min(src.len(), dst.len());
        dst[..len].copy_from_slice(&src[..len]);
        return Ok(DecompressResult {
            consumed: len,
            wrote: len,
            need: (dst.len() - len),
        });
    }
    fn reset(&mut self) -> McapResult<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        return "";
    }
}
