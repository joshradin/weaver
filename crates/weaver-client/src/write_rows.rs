use std::io::Write;
use std::time::Duration;

use tabled::builder::Builder;
use tabled::grid::config::HorizontalLine;
use tabled::settings::Style;

use weaver_core::rows::Rows;

pub fn write_rows<'r, W: Write, R: Rows<'r>>(
    mut write: W,
    mut rows: R,
    time: Duration,
) -> eyre::Result<()> {
    let schema = rows.schema();

    let mut builder = Builder::new();
    builder.push_record(schema.columns().iter().map(|col| col.name()));

    let mut row_count = 0;
    while let Some(row) = rows.next() {
        builder.push_record(row.iter().map(|v| v.to_string()));
        row_count += 1;
    }

    let table = builder
        .build()
        .with(Style::ascii().remove_horizontal().horizontals([(
            1,
            HorizontalLine::new(Some('-'), Some('+'), Some('+'), Some('+')).into(),
        )]))
        .to_string();

    writeln!(write, "{table}")?;
    writeln!(
        write,
        "{row_count} {} in set ({:0.2} sec)",
        pluralizer::pluralize("row", row_count, false),
        time.as_secs_f64()
    )?;

    Ok(())
}
