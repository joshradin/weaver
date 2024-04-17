use clap::Parser;
use color_eyre::eyre;
use std::thread;
use tempfile::tempdir;
use weaver_daemon::{run, cli::App};

#[test]
fn startup() -> eyre::Result<()> {
    let dir = tempdir()?;
    thread::spawn(|| {
        run(App::parse_from([
            "weaverd",
            dir.into_path().to_str().unwrap(),
            "-vv",
        ]))
    });

    Ok(())
}
