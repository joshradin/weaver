use std::path::{Path, PathBuf};

use clap::{value_parser, ArgAction, Parser};
use tracing::level_filters::LevelFilter;

/// App args
#[derive(Debug, Parser)]
pub struct App {
    /// The working directory
    pub working_dir: Option<PathBuf>,
    /// Sets the host ip for this
    #[clap(long, default_value = "localhost")]
    pub host: String,
    /// Sets the port to expose the weaver instance on
    #[clap(long, short = 'P', default_value_t = weaver_core::cnxn::DEFAULT_PORT)]
    pub port: u16,
    /// Sets the number of workers
    #[clap(long)]
    pub num_workers: Option<usize>,

    /// Sets the location of the key store
    #[clap(long)]
    key_store: Option<PathBuf>,

    /// Sets the verbosity of the application
    #[clap(short)]
    #[clap(action = ArgAction::Count, value_parser = value_parser!(u8).range(0..=2))]
    pub verbosity: u8,

    /// Automatically kills the instance after some amount of seconds
    #[clap(long, value_parser=value_parser!(u64).range(1..))]
    pub kill: Option<u64>,
}

impl App {
    pub fn level_filter(&self) -> LevelFilter {
        match self.verbosity {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            2.. => LevelFilter::TRACE,
        }
    }

    pub fn work_dir(&self) -> &Path {
        self.working_dir
            .as_ref()
            .map(|p| p.as_ref())
            .unwrap_or(Path::new("."))
    }

    pub fn key_store(&self) -> PathBuf {
        self.key_store
            .clone()
            .unwrap_or_else(|| self.work_dir().join("keys"))
    }
}
