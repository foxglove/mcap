// Future: add serde traits when needed

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Human,
    Json,
    Table,
}

#[derive(Debug, Clone)]
pub struct FormatOptions {
    pub format: OutputFormat,
    pub colored: bool,
    pub compact: bool,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            format: OutputFormat::Human,
            colored: true,
            compact: false,
        }
    }
}
