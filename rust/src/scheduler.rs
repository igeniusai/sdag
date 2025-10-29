use crate::backend::SlurmBackend;
use crate::model::{DAG, JobStatus, Node};
use crate::state::{LocalDirState, StateManager};
use crate::status_management;
use crate::submission::Submitter;
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
        self.state.prepare(&dag);
        let (root_id, mut nodemap) = self.build_nodemap(dag);
        let submitter = Submitter {
            state: self.state.clone(),
            backend: self.backend.clone(),
        };

        loop {
            status_management::update_status(&root_id, &mut nodemap, &self.backend);
            submitter.submit(&root_id, &mut nodemap);
            if self.is_simulation_completed(&nodemap) {
                break;
            }

            thread::sleep(self.poll_time);
        }
    }

    fn build_nodemap(&self, dag: DAG) -> (String, HashMap<String, Node>) {
        let mut nodemap = HashMap::new();
        for node in dag.nodes {
            nodemap.insert(node.uid.clone(), node);
        }

        let root_id = self.find_root_id(&nodemap);
        let root = nodemap.get_mut(&root_id).unwrap();
        root.status = JobStatus::Completed;
        (root_id, nodemap)
    }

    fn is_simulation_completed(&self, nodemap: &HashMap<String, Node>) -> bool {
        nodemap.values().all(|n| {
            matches!(n.status, JobStatus::Completed)
                | matches!(n.status, JobStatus::Failed)
                | matches!(n.status, JobStatus::Skipped)
        })
    }

    fn find_root_id(&self, nodemap: &HashMap<String, Node>) -> String {
        nodemap
            .values()
            .filter(|node| node.parents.len() == 0)
            .map(|node| &node.uid)
            .next()
            .expect("Pipeline has no root!")
            .to_string()
    }
}
