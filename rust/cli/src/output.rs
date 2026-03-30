#![allow(dead_code)]

use std::io::{self, Write};

use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(rows: Vec<Vec<String>>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[Vec<String>] {
        &self.rows
    }
}

pub fn emit_table(table: &Table) -> Result<()> {
    let mut out = io::stdout().lock();

    for row in table.rows() {
        writeln!(out, "{}", row.join("\t"))?;
    }

    Ok(())
}

pub fn print_rows(rows: &[Vec<String>]) -> Result<()> {
    emit_table(&Table::new(rows.to_vec()))
}
