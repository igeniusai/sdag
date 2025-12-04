//! # Scheduler
//!
//! The scheduler exists the infinite loop only
//! if every node in a final state.
//!
//! # Panics
//!
//! The scheduler panics if the nodemap cannot be build out
//! of the DAG.

use crate::backend::Backend;
use crate::checkpoint::Checkpointer;
use crate::graph;
use crate::model::{DAG, JobStatus, Node};
use crate::state::StateManager;
use crate::status_management;
use crate::submission::Submitter;
use crate::summary;
use log;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

/// Scheduler.
#[derive(Debug, Clone)]
pub struct Scheduler<T: StateManager, U: Backend> {
    /// Time between subsequent Slurm polls (s).
    pub poll_time: Duration,
    /// State management.
    pub state: T,
    /// Backend to submit and monitor jobs.
    pub backend: U,
    /// Checkpoint mgmt
    pub checkpointer: Checkpointer,
    /// Maximum number of concurrent tasks.
    pub max_concurrency: usize,
}
impl<T: StateManager, U: Backend> Scheduler<T, U> {
    /// Run the scheduler.
    pub fn run(&self, dag: DAG<Node>) {
        let (root_id, mut nodemap) = graph::build_nodemap(dag)
            .ok_or_else(|| log::error!("Failed to identify the root node"))
            .unwrap();

        let mut submitter = Submitter::new(&self.backend, &self.state, self.max_concurrency);
        log::debug!("Starting scheduling loop");

        loop {
            status_management::update_status(&root_id, &mut nodemap, &self.backend, &self.state);
            submitter.submit(&mut nodemap);

            let res = self.checkpointer.save_checkpoint(&nodemap, &self.state);
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
                | matches!(n.status, JobStatus::Failed)
                | matches!(n.status, JobStatus::Skipped)
        })
    }
}
