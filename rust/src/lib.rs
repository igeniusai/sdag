//! # Slurm DAG scheduler
//!
//! It can be used to schedule dynamic DAGs on Slurm clusters. The
//! scheduler takes the JSON representation of the DAG as input and
//! schedules task based on the graph structure.

use pyo3::prelude::*;
pub mod backend;
mod caching;
mod checkpoint;
pub mod graph;
pub mod model;
pub mod status_management;
pub mod stop_simulation;
pub mod submission;
mod summary;
use crate::state::{LocalDirState, StateManager};
use std::path::PathBuf;
mod limiters;
mod scheduler;
mod startup;
mod state;

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
fn start_scheduler(
    pipeline: PathBuf,
    wait_seconds: u64,
    max_concurrency: usize,
    log_level: String,
    local: bool,
    restart: bool,
) {
    startup::configure_logging(&log_level);
    scheduler::start_simulation(pipeline, wait_seconds, max_concurrency, local, restart);
}

#[pyfunction]
fn kill_pipeline(pipeline: String, log_level: String) {
    startup::configure_logging(&log_level);
    stop_simulation::kill_running_jobs(pipeline);
}

#[pyfunction]
fn prune_cache(task: String, log_level: String) {
    startup::configure_logging(&log_level);
    let home_dir = startup::find_home_dir()
        .map_err(|e| log::error!("{e}"))
        .unwrap();

    LocalDirState::new(&home_dir, "")
        .clear_cache(&task)
        .map_err(|e| log::error!("Failed to delete cache: {e}"))
        .unwrap();
}

/// Interface between Python and Rust.
#[pymodule]
// Function name must match `lib.name` in `Cargo.toml`
fn sscheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_scheduler, m)?)?;
    m.add_function(wrap_pyfunction!(kill_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(prune_cache, m)?)?;

    Ok(())
}
