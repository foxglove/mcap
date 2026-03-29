#![allow(dead_code)]

use std::io::{self, Write};

use anyhow::Result;

pub fn print_rows(rows: &[Vec<String>]) -> Result<()> {
    let mut out = io::stdout().lock();

    for row in rows {
        writeln!(out, "{}", row.join("\t"))?;
    }

    Ok(())
}
