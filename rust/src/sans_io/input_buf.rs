pub struct InputBuf<'a> {
    pub buf: &'a mut [u8],
    pub(crate) total_filled: &'a mut usize,
    pub(crate) at_eof: &'a mut bool,
}

impl<'a> InputBuf<'a> {
    pub fn set_filled(&'a mut self, written: usize) {
        *self.total_filled += written;
        *self.at_eof = written == 0;
    }
}
