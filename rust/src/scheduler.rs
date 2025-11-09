use crate::backend::SlurmBackend;
use crate::graph;
use crate::model::{DAG, JobStatus, Node};
use crate::state::LocalDirState;
use crate::status_management;
use crate::submission::Submitter;
use crate::summary;
use log;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub poll_time: Duration,
    pub state: LocalDirState,
    pub backend: SlurmBackend,
}
impl Scheduler {
    pub fn run(&self, dag: DAG) {
        let (root_id, mut nodemap) = graph::build_nodemap(dag)
            .ok_or_else(|| log::error!("Failed to identify the root node"))
            .unwrap();

        let submitter = Submitter {
            state: &self.state,
            backend: &self.backend,
        };

        log::debug!("Starting scheduling loop");
        loop {
            status_management::update_status(&root_id, &mut nodemap, &self.backend);
            submitter.submit(&mut nodemap);

            let table = summary::get_summary_table(&nodemap);
            log::info!("Summary:\n{table}");

            if self.is_simulation_completed(&nodemap) {
                log::info!("Simulation completed, exiting...");
                break;
            }

            thread::sleep(self.poll_time);
        }
    }

    fn is_simulation_completed(&self, nodemap: &HashMap<String, Node>) -> bool {
        nodemap.values().all(|n| {
            matches!(n.status, JobStatus::Completed { .. })
                | matches!(n.status, JobStatus::Failed)
                | matches!(n.status, JobStatus::Skipped)
        })
    }
}
