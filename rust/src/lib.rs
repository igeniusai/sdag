use crate::model::DAG;
use crate::state::StateManager;
use pyo3::prelude::*;
use serde_json;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
mod model;
mod summary;

mod backend;
mod status_management;
mod submission;

use std::time;
mod parser;
use clap::Parser;
use std::env;

mod state;
use state::LocalDirState;
mod scheduler;
use scheduler::Scheduler;

fn read_dag(path: &PathBuf) -> io::Result<DAG> {
    let mut f = File::open(path)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    let dag: DAG = serde_json::from_str(&buf)?;
    Ok(dag)
}

fn find_home_dir() -> Result<PathBuf, ()> {
    match env::var("SDAG_HOME") {
        Ok(val) => Ok(PathBuf::from(val)),
        Err(_) => {
            let path = env::home_dir().ok_or(())?;
            Ok(path.join(".sdag"))
        }
    }
}

#[pyfunction]
fn sscheduler_start(argv: Vec<String>) {
    let args = parser::CLI::parse_from(argv);
    let home_dir =
        find_home_dir().expect("SDAG_HOME env variable not set and home directory not found.");

    let dag = read_dag(&args.pipeline).expect("Failed to parse DAG from JSON file.");
    let state = LocalDirState::new(home_dir.join(&dag.name));
    state
        .prepare(&dag)
        .expect("Failed to prepare the working directory.");

    state
        .copy_dag_into_working_dir(&args.pipeline)
        .expect("Failed to copy the pipeline DAG into the working directory.");

    let backend = backend::SlurmBackend;
    let poll_time = time::Duration::from_secs(args.wait_seconds);
    let scheduler = Scheduler {
        poll_time,
        state,
        backend,
    };

    scheduler.run(dag);
}

// Function name must match `lib.name` in `Cargo.toml`
#[pymodule]
fn sscheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sscheduler_start, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_home() {
        let path = "/a/path";
        unsafe {
            env::set_var("SDAG_HOME", path);
        }
        let home_dir = find_home_dir().unwrap();
        assert_eq!(home_dir, PathBuf::from(path));
    }
}
