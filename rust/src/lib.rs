//! # Slurm DAG scheduler
//!
//! It can be used to schedule dynamic DAGs on Slurm clusters. The
//! scheduler takes the JSON representation of the DAG as input and
//! schedules task based on the graph structure.

use crate::state::StateManager;
use pyo3::prelude::*;
pub mod backend;
mod caching;
mod checkpoint;
pub mod graph;
pub mod model;
pub mod status_management;
pub mod submission;
mod summary;

use std::time;
mod parser;
use checkpoint::Checkpointer;
use clap::Parser;

mod state;
use state::LocalDirState;
mod scheduler;
use log;
use scheduler::Scheduler;
mod startup;

/// Start the scheduler.
///
/// It takes as input the input arguments sent by the user. They are parsed
/// and used to configure and start the scheduler.
///
/// # Panics
///
/// - The home directory cannot be found and `SDAG_HOME` is not set.
/// - The pipeline JSON graph cannot be parsed.
/// - The working directory cannot be built for any reason.
/// - The JSON file cannot be copied into the working directory.
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

    let state = LocalDirState::new(home_dir.join(&dag.meta.name));
    log::info!("Working directory: '{:?}'", state.get_pipeline_dir());

    let checkpointer = Checkpointer {
        meta: dag.meta.clone(),
        restart: args.restart,
    };

    let dag = startup::prepare_dag(dag, &args.pipeline, &checkpointer, &state)
        .map_err(|e| log::error!("Failed to prepare DAG: {e}"))
        .unwrap();

    let backend = backend::SlurmBackend;
    let poll_time = time::Duration::from_secs(args.wait_seconds);
    let scheduler = Scheduler {
        poll_time,
        state,
        backend,
        checkpointer,
        max_concurrency: args.max_concurrency,
    };

    log::info!("Running pipeline '{}'", dag.meta.name);
    scheduler.run(dag);
}

/// Interface between Python and Rust.
#[pymodule]
// Function name must match `lib.name` in `Cargo.toml`
fn sscheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sscheduler_start, m)?)?;
    Ok(())
}
