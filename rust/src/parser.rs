//! sdag scheduler CLI.

use clap::{ArgAction, Parser};
use std::path::PathBuf;

/// Scheduler CLI.
#[derive(Parser, Debug)]
#[command(name = "sscheduler")]
#[command(version = "0.1.0")]
#[command(about = "Slurm job scheduler", long_about = None)]
pub struct CLI {
    #[arg(short, long, value_name = "FILE", help = "Compiled pipeline path")]
    pub pipeline: PathBuf,
    #[arg(
        short,
        long,
        default_value_t = 5,
        help = "Time between successive Slurm polls"
    )]
    pub wait_seconds: u64,
    #[arg(
        short,
        long,
        default_value_t = 0,
        help = "Max number of concurrent tasks. Set to 0 to disable."
    )]
    pub max_concurrency: usize,
    #[arg(
        short,
        long,
        default_value_t = String::from("info"),
        help = "Logging level (debug, info, error, or tracing)"
    )]
    pub log_level: String,
    #[arg(
        short,
        long,
        action=ArgAction::SetTrue,
        help = "Restart a pipeline from the last checkpoint"
    )]
    pub restart: bool,
    #[arg(
        long,
        action=ArgAction::SetTrue,
        help = "Submit jobs locally as blocking, child processes."
    )]
    pub local: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Arguments are parsed correctly.
    #[test]
    fn parse_args() {
        let iter = ["sscheduler", "--pipeline=pipeline.json", "--wait-seconds=2"].iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert_eq!(cli.pipeline, PathBuf::from("pipeline.json"));
        assert_eq!(cli.wait_seconds, 2);
        assert_eq!(cli.max_concurrency, 0);
        assert_eq!(cli.restart, false);
    }

    /// The -p/--pipeline is mandatory.
    #[test]
    #[should_panic]
    fn pipeline_is_missing() {
        let iter = ["sscheduler", "--wait-seconds=2"].iter();
        CLI::try_parse_from(iter).unwrap();
    }

    /// Non-default log level is correctly captured.
    #[test]
    fn parse_log_level() {
        let iter = [
            "sscheduler",
            "--pipeline=pipeline.json",
            "--log-level=debug",
        ]
        .iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert_eq!(cli.log_level, String::from("debug"));
    }

    /// Non-default log level is correctly captured.
    #[test]
    fn parse_max_concurrency() {
        let iter = [
            "sscheduler",
            "--pipeline=pipeline.json",
            "--max-concurrency=3",
        ]
        .iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert_eq!(cli.max_concurrency, 3);
    }

    /// Check the restart flag is correctly parsed.
    #[test]
    fn parse_restart() {
        let iter = ["sscheduler", "--pipeline=pipeline.json", "--restart"].iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert!(cli.restart);
    }

    /// Check the restart flag is correctly parsed.
    #[test]
    fn parse_local() {
        let iter = ["sscheduler", "--pipeline=pipeline.json", "--local"].iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert!(cli.local);
    }
}
