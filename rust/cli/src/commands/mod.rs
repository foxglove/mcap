// Primary commands
pub mod cat;
pub mod convert;
pub mod doctor;
pub mod filter;
pub mod info;
pub mod merge;
pub mod recover;
pub mod sort;

// List subcommands
pub mod list {
    pub mod attachments;
    pub mod channels;
    pub mod chunks;
    pub mod schemas;
}

// Get subcommands
pub mod get {
    pub mod attachment;
}

// Add subcommands
pub mod add {
    pub mod attachment;
    pub mod metadata;
}

// Utility commands
pub mod compression;
pub mod du;
pub mod version;
