use crate::model::DAG;
use crate::state::StateManager;
use pyo3::prelude::*;
use serde_json;
use std::fs;
use std::io;
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
use env_logger::Env;
use log;
use scheduler::Scheduler;

fn read_dag(path: &PathBuf) -> io::Result<DAG> {
    let buf = fs::read_to_string(path)?;
    let dag: DAG = serde_json::from_str(&buf)?;
    Ok(dag)
}

fn find_home_dir() -> Result<PathBuf, String> {
    match env::var("SDAG_HOME") {
        Ok(val) => Ok(PathBuf::from(val)),
        Err(_) => {
            let path = env::home_dir().ok_or(String::from(
                "Failed to identify a home directory. Please Set the \
            'SDAG_HOME' environment variable.",
            ))?;
            Ok(path.join(".sdag"))
        }
    }
}

fn configure_logging(log_level: &str) {
    let env = Env::default()
        .filter_or("SDAG_LOG_LEVEL", log_level)
        .write_style_or("SDAG_LOG_STYLE", "always");

    env_logger::init_from_env(env);
    log::debug!("Logging configured")
}

#[pyfunction]
fn sscheduler_start(argv: Vec<String>) {
    let args = parser::CLI::parse_from(argv);
    configure_logging(&args.log_level);

    let home_dir = find_home_dir().map_err(|e| log::error!("{e}")).unwrap();
    log::debug!("SDAG home directory: {home_dir:?}");

    let dag = read_dag(&args.pipeline)
        .map_err(|e| log::error!("{e}"))
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
