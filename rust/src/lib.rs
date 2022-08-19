pub mod records;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = records::OpCode::Header;
        assert_eq!(format!("{:?}", result), "Header");
    }
}
