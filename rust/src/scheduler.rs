//! # Scheduler
//!
//! The scheduler exists the infinite loop only
//! if every node in a final state.
//!
//! # Panics
//!
//! The scheduler panics if the nodemap cannot be build out
//! of the DAG.

use crate::backend::{Backend, SchedulerBackend};
use crate::checkpoint::Checkpointer;
use crate::graph;
use crate::model::{DAG, JobStatus, Node};
use crate::startup;
use crate::state::{LocalDirState, StateManager};
use crate::status_management;
use crate::submission::Submitter;
use crate::summary;
use log;
use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

pub fn start_simulation(
    pipeline: PathBuf,
    wait_seconds: u64,
    max_concurrency: usize,
    local: bool,
    restart: bool,
) {
    let home_dir = startup::find_home_dir()
        .map_err(|e| log::error!("{e}"))
        .unwrap();

    log::debug!("SDAG home directory: {home_dir:?}");
    let dag = startup::read_dag(&pipeline)
        .map_err(|e| log::error!("Failed to read DAG: {e}"))
        .unwrap();

    let state = LocalDirState::new(&home_dir, &dag.meta.name);
    log::info!("Working directory: '{:?}'", state.get_pipeline_dir());

    let checkpointer = Checkpointer {
        meta: dag.meta.clone(),
        restart,
    };

    let dag = startup::prepare_dag(dag, &local, &pipeline, &checkpointer, &state)
        .map_err(|e| log::error!("Failed to prepare DAG: {e}"))
        .unwrap();

    let pipeline_name = dag.meta.name.clone();
    let backend = SchedulerBackend::new(&pipeline_name, &state);
    let poll_time = Duration::from_secs(wait_seconds);
    let mut scheduler = Scheduler {
        poll_time,
        state: &state,
        checkpointer,
        max_concurrency,
    };

    log::info!("Running pipeline '{}'", dag.meta.name);
    scheduler.run(dag, backend);
}

/// Scheduler.
#[derive(Debug, Clone)]
pub struct Scheduler<'a, T: StateManager> {
    /// Time between subsequent Slurm polls (s).
    pub poll_time: Duration,
    /// State management.
    pub state: &'a T,
    /// Checkpoint mgmt
    pub checkpointer: Checkpointer,
    /// Maximum number of concurrent tasks.
    pub max_concurrency: usize,
}
impl<'a, T: StateManager> Scheduler<'a, T> {
    /// Run the scheduler.
    pub fn run(&mut self, dag: DAG<Node>, mut backend: impl Backend) {
        let (root_id, mut nodemap) = graph::build_nodemap(dag)
            .ok_or_else(|| log::error!("Failed to identify the root node"))
            .unwrap();

        let mut submitter = Submitter::new(self.state, self.max_concurrency);
        log::debug!("Starting scheduling loop");

        loop {
            backend.update_status(&mut nodemap);
            status_management::update_status(&root_id, &mut nodemap);
            submitter.submit(&mut nodemap, &backend);

            let res = self.checkpointer.save_checkpoint(&nodemap, self.state);
            if let Err(e) = res {
                log::error!("Failed to save checkpoint: {e}");
            }

            let table = summary::get_summary_table(&nodemap);
            log::info!("Summary:\n{table}");

            if self.is_simulation_completed(&nodemap) {
                log::info!("Simulation completed, exiting...");
                break;
            }

            thread::sleep(self.poll_time);
        }
    }

    /// Every node is in a final state (completed, failed, or skipped).
    fn is_simulation_completed(&self, nodemap: &HashMap<String, Node>) -> bool {
        nodemap.values().all(|n| {
            matches!(n.status, JobStatus::Completed { .. })
                | matches!(n.status, JobStatus::Failed(_))
                | matches!(n.status, JobStatus::Skipped)
        })
    }
}
