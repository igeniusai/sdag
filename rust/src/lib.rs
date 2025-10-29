use crate::model::{DAG, JobStatus, Node};
use pyo3::prelude::*;
use serde_json;
use std::path::PathBuf;
use std::{fs::File, io::Read};
mod model;

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

fn read_dag(path: &PathBuf) -> DAG {
    let mut f = File::open(path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    let dag: DAG = serde_json::from_str(&buf).unwrap();
    dag
}

fn find_home_dir() -> PathBuf {
    match env::var("SDAG_HOME") {
        Ok(val) => PathBuf::from(val),
        Err(_) => {
            let err_msg = "SDAG_HOME not set and home directory cannot be identified";
            let path = env::home_dir().expect(&err_msg);
            path.join(".sdag")
        }
    }
}

#[pyfunction]
fn sscheduler_start(argv: Vec<String>) {
    let args = parser::CLI::parse_from(argv);
    let home_dir = find_home_dir();
    let dag = read_dag(&args.pipeline);
    let backend = backend::SlurmBackend;
    let state = LocalDirState::new(home_dir.join(&dag.name));
    let poll_time = time::Duration::from_secs(args.wait_seconds);

    let scheduler = Scheduler {
        poll_time,
        state,
        backend,
    };

    scheduler.run(dag);
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn sscheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sscheduler_start, m)?)?;

    Ok(())
}
