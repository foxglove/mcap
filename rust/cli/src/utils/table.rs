use comfy_table::{Attribute, Cell, Table};
use std::io::{self, Write};

/// Format and print a table with rows of data
pub fn format_table<W: Write>(writer: &mut W, rows: &[Vec<String>]) -> io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);

    // Add header if available
    if let Some(header) = rows.first() {
        let header_cells: Vec<Cell> = header
            .iter()
            .map(|cell| Cell::new(cell).add_attribute(Attribute::Bold))
            .collect();
        table.set_header(header_cells);
    }

    // Add data rows
    for row in rows.iter().skip(1) {
        table.add_row(row);
    }

    writeln!(writer, "{}", table)?;
    Ok(())
}

/// Format summary rows (key-value pairs) similar to the Go CLI info output
pub fn format_summary_rows(rows: &[(String, String)]) {
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.set_content_arrangement(comfy_table::ContentArrangement::DynamicFullWidth);

    for (key, value) in rows {
        table.add_row(vec![key, value]);
    }

    println!("{}", table);
}
