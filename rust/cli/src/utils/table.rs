use comfy_table::{Cell, Table};

pub fn create_table() -> Table {
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL);
    table
}

pub fn add_row(table: &mut Table, cells: Vec<String>) {
    let cells: Vec<Cell> = cells.into_iter().map(Cell::new).collect();
    table.add_row(cells);
}
