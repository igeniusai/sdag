use crate::state::StateManager;
use pyo3::prelude::*;
mod model;
mod summary;

mod backend;
mod graph;
mod status_management;
mod submission;

use std::time;
mod parser;
use clap::Parser;

mod state;
use state::LocalDirState;
mod scheduler;
use log;
use scheduler::Scheduler;
mod input_data;
mod startup;

#[pyfunction]
fn sscheduler_start(argv: Vec<String>) {
    let args = parser::CLI::parse_from(argv);
    startup::configure_logging(&args.log_level);

    let home_dir = startup::find_home_dir()
        .map_err(|e| log::error!("{e}"))
        .unwrap();

    log::debug!("SDAG home directory: {home_dir:?}");

    let dag = startup::read_dag(&args.pipeline)
        .map_err(|e| log::error!("Failed to read DAG: {e}"))
        .unwrap();

    let state = LocalDirState::new(home_dir.join(&dag.name));
    log::info!("Working directory: '{:?}'", state.get_pipeline_dir());

    state
        .prepare(&dag)
        .map_err(|e| log::error!("Working directory creation failed: {e}."))
        .unwrap();

    state
        .copy_dag_into_working_dir(&args.pipeline)
        .map_err(|e| log::error!("Failed to copy DAG file: {e}"))
        .unwrap();

    let backend = backend::SlurmBackend;
    let poll_time = time::Duration::from_secs(args.wait_seconds);
    let scheduler = Scheduler {
        poll_time,
        state,
        backend,
    };

    log::info!("Running pipeline '{}'", dag.name);
    scheduler.run(dag);
}

// Function name must match `lib.name` in `Cargo.toml`
#[pymodule]
fn sscheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sscheduler_start, m)?)?;
    Ok(())
}
