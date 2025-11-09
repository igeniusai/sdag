use crate::backend::SlurmBackend;
use crate::model::{DAG, JobStatus, Node, NodeResult};
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
        let (root_id, mut nodemap) = self
            .build_nodemap(dag)
            .ok_or_else(|| log::error!("Failed to identify the root node"))
            .unwrap();
        self.add_child_edges(&mut nodemap);

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

    fn build_nodemap(&self, dag: DAG) -> Option<(String, HashMap<String, Node>)> {
        let mut nodemap = HashMap::new();
        for node in dag.nodes {
            nodemap.insert(node.uid.clone(), node);
        }

        let root_id = self.find_root_id(&nodemap)?;
        let root = nodemap.get_mut(&root_id)?;
        root.status = JobStatus::Completed(NodeResult::Node);
        Some((root_id, nodemap))
    }

    fn is_simulation_completed(&self, nodemap: &HashMap<String, Node>) -> bool {
        nodemap.values().all(|n| {
            matches!(n.status, JobStatus::Completed { .. })
                | matches!(n.status, JobStatus::Failed)
                | matches!(n.status, JobStatus::Skipped)
        })
    }

    fn find_root_id(&self, nodemap: &HashMap<String, Node>) -> Option<String> {
        nodemap
            .values()
            .filter(|node| node.parents.len() == 0)
            .map(|node| &node.uid)
            .map(|uid| uid.to_string())
            .next()
    }

    fn add_child_edges(&self, nodemap: &mut HashMap<String, Node>) {
        let mut edges = self.find_child_edges(nodemap);
        for (uid, node) in nodemap.iter_mut() {
            if let Some(children) = edges.remove(uid) {
                node.children.extend(children);
            }
        }
    }

    fn find_child_edges(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, Vec<String>> {
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for (uid, node) in nodemap.iter() {
            for parent in &node.parents {
                let parent_vec = children.entry(parent.uid.clone()).or_default();
                if !parent_vec.contains(uid) {
                    parent_vec.push(uid.clone())
                }
            }
        }
        children
    }
}
