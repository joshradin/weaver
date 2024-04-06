use std::thread;
use clap::Parser;
use color_eyre::eyre;
use tempfile::tempdir;
use weaver_daemon::{App, run};

#[test]
fn startup() -> eyre::Result<()> {
    let dir= tempdir()?;
    thread::spawn(|| {
        run(App::parse_from(["weaverd", dir.into_path().to_str().unwrap(), "-vv"]))
    });

    Ok(())
}