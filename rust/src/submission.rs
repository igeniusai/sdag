//! Node submission.
//!
//! The submission depends on the node type. Tasks are executed as
//! jobs, other types are managed by the scheduler.

use crate::backend::Backend;
use crate::input_data::InputDataHandler;
use crate::model::{BooleanOutput, InputKwarg, JobStatus, Node, NodeBehavior, NodeResult};
use crate::state::StateManager;
use crate::status_management::StatusSelector;
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::io;

/// Execute nodes.
#[derive(Debug, Clone)]
pub struct Submitter<'a, T: Backend, U: StateManager> {
    /// Task submission backend.
    pub backend: &'a T,
    /// File system interaction.
    pub state: &'a U,
}

impl<'a, T: Backend, U: StateManager> Submitter<'a, T, U> {
    /// Execute nodes ready for submission.
    pub fn submit(&self, nodemap: &mut HashMap<String, Node>) {
        let updated_statuses = self.find_updated_statuses(nodemap);
        self.update_status(nodemap, updated_statuses);
    }

    /// Find nodes ready for submission.
    fn find_updated_statuses(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, JobStatus> {
        nodemap
            .iter()
            .filter(|(_, node)| matches!(node.status, JobStatus::ReadyForSubmission))
            .map(|(k, _)| (k.clone(), self.submit_node(k, nodemap)))
            .collect()
    }

    /// Update the status of all nodes after the submission.
    fn update_status(
        &self,
        nodemap: &mut HashMap<String, Node>,
        updated_statuses: HashMap<String, JobStatus>,
    ) {
        for (k, v) in updated_statuses.into_iter() {
            let updated_node = nodemap.get_mut(&k).unwrap();
            updated_node.status = v;
        }
    }

    /// Submit a node based on its behavior.
    fn submit_node(&self, uid: &str, nodemap: &HashMap<String, Node>) -> JobStatus {
        let node = nodemap.get(uid).unwrap();
        match &node.behavior {
            NodeBehavior::RootNode { .. } | NodeBehavior::EndNode { .. } => {
                JobStatus::Completed(NodeResult::Node)
            }
            NodeBehavior::TaskNode {
                launch_script,
                caching,
                try_num,
                input_kwargs,
                ..
            } => self.submit_tasknode(
                &node.uid,
                nodemap,
                input_kwargs,
                launch_script,
                caching,
                try_num,
            ),
            NodeBehavior::OneOfNode { .. } => match self.submit_oneofnode(&node.uid, &nodemap) {
                Ok(uid) => JobStatus::Completed(NodeResult::OneOf(uid)),
                Err(_) => JobStatus::Failed,
            },
            NodeBehavior::IfNode { .. } => match self.submit_ifnode(&node.uid, &nodemap) {
                Err(_) => JobStatus::Failed,
                Ok(branch) => JobStatus::Completed(NodeResult::If(branch)),
            },
        }
    }

    /// Submit a task.
    fn submit_tasknode(
        &self,
        uid: &str,
        nodemap: &HashMap<String, Node>,
        input_kwargs: &Vec<InputKwarg>,
        launch_script: &str,
        caching: &bool,
        try_num: &u32,
    ) -> JobStatus {
        log::debug!("Submitting Task {uid}");
        let handler = InputDataHandler {
            uid,
            state: self.state,
        };

        match handler.read_input_data(nodemap, input_kwargs) {
            Err(e) => {
                log::error!("Failed to read {uid} input data: {e}");
                return JobStatus::Failed;
            }
            Ok(input) => {
                if *caching && handler.is_cached(&input) {
                    log::info!("Task {uid} is cached");
                    return JobStatus::Completed(NodeResult::Node);
                }

                if let Err(_) = handler.save_input(&input) {
                    log::error!("Task {uid}: Failed to save input data");
                    return JobStatus::Failed;
                }
            }
        }

        let pipeline_dir = self.state.get_pipeline_dir();
        let res = self
            .backend
            .submit(launch_script, uid, pipeline_dir, try_num);
        match res {
            Ok(job_id) => JobStatus::Running(job_id),
            Err(e) => {
                log::error!("Failed task {uid} submission: {e}");
                JobStatus::Failed
            }
        }
    }

    /// Execute an IfNode and return the selected branch.
    fn submit_ifnode(
        &self,
        uid: &str,
        nodemap: &HashMap<String, Node>,
    ) -> Result<bool, Box<dyn Error>> {
        let parent_statuses = StatusSelector::get_parent_statuses(uid, nodemap);
        let parent_uid = self.find_completed_parent_uid(&parent_statuses);
        let output = self.state.read_output(&parent_uid)?;
        let choice: BooleanOutput = serde_json::from_str(&output)?;
        Ok(choice.output)
    }

    /// Submit a OneOf node.
    fn submit_oneofnode(&self, uid: &str, nodemap: &HashMap<String, Node>) -> io::Result<String> {
        let parent_statuses = StatusSelector::get_parent_statuses(uid, nodemap);
        let parent_uid = self.find_completed_parent_uid(&parent_statuses);
        self.state.copy_output(&parent_uid, uid)?;
        Ok(parent_uid)
    }

    /// Find the uid of a completed parent.
    ///
    /// # Panics
    /// The completed parent must exist as it has already been checked.
    fn find_completed_parent_uid(&self, parent_statuses: &HashMap<String, JobStatus>) -> String {
        parent_statuses
            .iter()
            .filter(|x| matches!(x.1, JobStatus::Completed { .. }))
            .map(|x| x.0.to_string())
            .next()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{Parent, ParentType, DAG};

    use super::*;
    use std::path::PathBuf;

    /// Mocked backend for testing purposes.
    struct MockBackend;
    impl Backend for MockBackend {
        fn update_status(&self, _nodemap: &mut HashMap<String, Node>) {}
        fn submit(
            &self,
            _launch_script: &str,
            _uid: &str,
            _pipeline_dir: &PathBuf,
            _try_num: &u32,
        ) -> Result<String, Box<dyn Error>> {
            Ok(String::from("1234"))
        }
    }

    /// Mocked state for testing purposes.
    struct MockState {
        fake_path: PathBuf,
    }
    impl MockState {
        fn new() -> Self {
            Self {
                fake_path: PathBuf::from("path"),
            }
        }
    }

    impl StateManager for MockState {
        fn prepare(&self, _dag: &DAG) -> io::Result<()> {
            Ok(())
        }
        fn copy_output(&self, _src_uid: &str, _dst_uid: &str) -> std::io::Result<u64> {
            std::io::Result::Ok(1)
        }
        fn read_output(&self, _uid: &str) -> std::io::Result<String> {
            std::io::Result::Ok(String::from(r#"{"output":true}"#))
        }
        fn get_pipeline_dir(&self) -> &PathBuf {
            &self.fake_path
        }
        fn copy_dag_into_working_dir(&self, _path: &PathBuf) -> io::Result<u64> {
            Ok(1)
        }
        fn save_input(&self, _uid: &str, _input: &str) -> io::Result<()> {
            Ok(())
        }
        fn read_cached_input(&self, _uid: &str) -> io::Result<String> {
            Ok(String::from(r#"{}"#))
        }
    }

    /// Find the completed parent uid for the OneOf node submission.
    #[test]
    fn find_parent_uid_for_oneof() {
        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };
        let parent_statuses = HashMap::from([
            (String::from("a"), JobStatus::NotSubmitted),
            (String::from("b"), JobStatus::Completed(NodeResult::Node)),
            (String::from("c"), JobStatus::Failed),
        ]);

        let uid = submitter.find_completed_parent_uid(&parent_statuses);
        assert_eq!(uid, String::from("b"));
    }

    /// A rootnode is just marked as completed.
    #[test]
    fn submit_root() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                parent_type: ParentType::Output {
                    key: String::from("p"),
                },
                uid: String::from("p"),
            }],
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    /// And endnode is just marked as completed.
    #[test]
    fn submit_end() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::EndNode,
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                parent_type: ParentType::Output {
                    key: String::from("p"),
                },
                uid: String::from("p"),
            }],
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    /// If successful, the result of the IfNode contains the selected branch.
    #[test]
    fn submit_if() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::IfNode,
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                parent_type: ParentType::Output {
                    key: String::from("p"),
                },
                uid: String::from("p"),
            }],
            children: Vec::new(),
        };

        let parent = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(
            new_status,
            JobStatus::Completed(NodeResult::If(true))
        ))
    }

    /// Submit a task with the help of the backend.
    #[test]
    fn submit_task() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: false,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                parent_type: ParentType::Output {
                    key: String::from("p"),
                },
                uid: String::from("p"),
            }],
            children: Vec::new(),
        };

        let parent = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Running(_)))
    }

    /// Submit a cached task, so no execution is required.
    #[test]
    fn submit_cached_task() {
        let node = Node {
            uid: String::from("_cached_"),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: true,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("_cached_".to_string(), node)]);
        let new_status = submitter.submit_node("_cached_", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    /// Submit a OneOf node.
    #[test]
    fn submit_oneof() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::OneOfNode,
            status: JobStatus::ReadyForSubmission,
            parents: vec![
                Parent {
                    parent_type: ParentType::Output {
                        key: String::from("p2"),
                    },
                    uid: String::from("p1"),
                },
                Parent {
                    parent_type: ParentType::Output {
                        key: String::from("p2"),
                    },
                    uid: String::from("p2"),
                },
            ],
            children: Vec::new(),
        };

        let p1 = Node {
            uid: String::from("p1"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Failed,
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let p2 = Node {
            uid: String::from("p2"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([
            ("c".to_string(), node),
            ("p1".to_string(), p1),
            ("p2".to_string(), p2),
        ]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(_)))
    }

    /// Update the status of a node after submission
    #[test]
    fn find_updated_statuses() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let updated_statuses = submitter.find_updated_statuses(&nodemap);
        assert_eq!(
            updated_statuses,
            HashMap::from([("c".to_string(), JobStatus::Completed(NodeResult::Node))])
        )
    }

    /// Update the status of a node.
    #[test]
    fn update_status() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let mut nodemap = HashMap::from([("c".to_string(), node)]);
        let updated_statuses = HashMap::from([("c".to_string(), JobStatus::Failed)]);

        submitter.update_status(&mut nodemap, updated_statuses);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::Failed))
    }

    /// Complete graph execution test.
    #[test]
    fn e2e() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let mut nodemap = HashMap::from([("c".to_string(), node)]);

        submitter.submit(&mut nodemap);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(
            child.status,
            JobStatus::Completed(NodeResult::Node)
        ))
    }
}
