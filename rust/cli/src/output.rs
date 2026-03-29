#![allow(dead_code)]

use std::io::{self, Write};

use anyhow::Result;

pub fn print_rows(rows: &[Vec<String>]) -> Result<()> {
    let mut out = io::stdout().lock();
    print_rows_to(&mut out, rows)
}

pub fn print_rows_to<W: Write>(out: &mut W, rows: &[Vec<String>]) -> Result<()> {
    for row in rows {
        writeln!(out, "{}", row.join("\t"))?;
    }

    Ok(())
}
