pub struct InputBuf<'a> {
    pub buf: &'a mut [u8],
    pub(crate) total_filled: &'a mut usize,
    pub(crate) at_eof: &'a mut bool,
    pub(crate) data_section_hasher: &'a mut Option<crc32fast::Hasher>,
}

impl<'a> InputBuf<'a> {
    pub fn set_filled(&'a mut self, written: usize) {
        if let Some(hasher) = self.data_section_hasher {
            hasher.update(&self.buf[..written]);
        }
        *self.total_filled += written;
        *self.at_eof = written == 0;
    }
    pub fn copy_from(&'a mut self, other: &[u8]) -> usize {
        let len = std::cmp::min(self.buf.len(), other.len());
        let src = &other[..len];
        let dst = &mut self.buf[..len];
        dst.copy_from_slice(src);
        self.set_filled(len);
        len
    }
}
