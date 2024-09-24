pub struct InputBuf<'a> {
    pub buf: &'a mut [u8],
    pub(crate) written: &'a mut usize,
}

impl<'a> InputBuf<'a> {
    pub fn set_filled(&'a mut self, written: usize) {
        *self.written += written;
    }
}
