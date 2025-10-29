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
}
