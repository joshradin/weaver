use prettytable::format::{LinePosition, LineSeparator, TableFormat};
use prettytable::{Cell, Row, Table};
use std::io::Write;
use std::time::Duration;
use weaver_core::rows::Rows;

pub fn write_rows<'r, W: Write, R: Rows<'r>>(
    mut write: W,
    mut rows: R,
    time: Duration,
) -> eyre::Result<()> {
    let schema = rows.schema();

    let mut table = Table::new();
    table.set_titles(Row::new(
        schema
            .columns()
            .iter()
            .map(|col| Cell::new(col.name()))
            .collect(),
    ));
    let mut format = TableFormat::default();

    format.separator(LinePosition::Title, LineSeparator::new('-', '+', '+', '+'));
    format.separator(LinePosition::Top, LineSeparator::new('-', '+', '+', '+'));
    format.separator(LinePosition::Bottom, LineSeparator::new('-', '+', '+', '+'));
    format.separator(LinePosition::Intern, LineSeparator::new(' ', '+', '+', '+'));
    format.padding(1, 1);
    format.column_separator('|');
    format.borders('|');

    table.set_format(format);

    let mut row_count = 0;
    while let Some(row) = rows.next() {
        table.add_row(Row::new(
            row.iter().map(|v| Cell::new(&v.to_string())).collect(),
        ));
        row_count += 1;
    }

    table.print(&mut write)?;
    writeln!(
        write,
        "{row_count} {} in set ({:0.2} sec)",
        pluralizer::pluralize("row", row_count, false),
        time.as_secs_f64()
    )?;

    Ok(())
}
