use clap::Parser;
use color_eyre::eyre;

use weaver_daemon::{run, App};

fn main() -> eyre::Result<()> {
    let app = App::parse();
    run(app)?;
    Ok(())
}
