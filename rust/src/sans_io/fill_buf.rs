pub struct FillBuf<'a> {
    pub buf: &'a mut [u8],
    pub(crate) written: &'a mut usize,
}

impl<'a> FillBuf<'a> {
    pub fn set_filled(&'a mut self, written: usize) {
        *self.written += written;
    }
}
