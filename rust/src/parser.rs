use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "sscheduler")]
#[command(version = "0.1.0")]
#[command(about = "Slurm job scheduler", long_about = None)]
pub struct CLI {
    #[arg(short, long, value_name = "FILE", help = "Compile pipeline path")]
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
        default_value_t = String::from("info"),
        help = "Logging level (debug, info, error, or tracing)"
    )]
    pub log_level: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args() {
        let iter = ["sscheduler", "--pipeline=pipeline.json", "--wait-seconds=2"].iter();
        let cli = CLI::try_parse_from(iter).unwrap();
        assert_eq!(cli.pipeline, PathBuf::from("pipeline.json"));
        assert_eq!(cli.wait_seconds, 2);
    }

    #[test]
    #[should_panic]
    fn pipeline_is_missing() {
        let iter = ["sscheduler", "--wait-seconds=2"].iter();
        CLI::try_parse_from(iter).unwrap();
    }

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
}
