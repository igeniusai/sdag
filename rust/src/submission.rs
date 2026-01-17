//! Node submission.
//!
//! The submission depends on the node type. Tasks are executed as
//! jobs, other types are managed by the scheduler.

use crate::backend::Backend;
use crate::caching;
use crate::limiters;
use crate::model::{BooleanOutput, JobStatus, Node, NodeBehavior, NodeFailure, NodeResult, Task};
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
    /// Maximum number of concurrent tasks.
    pub max_concurrency: usize,
    /// Number of running tasks.
    pub nrunning: usize,
}

impl<'a, T: Backend, U: StateManager> Submitter<'a, T, U> {
    /// Set the initial number of running tasks to zero.
    pub fn new(backend: &'a T, state: &'a U, max_concurrency: usize) -> Self
    where
        T: Backend,
        U: StateManager,
    {
        Self {
            backend,
            state,
            max_concurrency,
            nrunning: 0,
        }
    }

    /// Execute nodes ready for submission.
    pub fn submit(&mut self, nodemap: &mut HashMap<String, Node>) {
        self.set_number_of_running_jobs(nodemap);
        caching::read_input_and_cache_tasks(nodemap, self.state);
        limiters::limit_cached_tasks_same_name(nodemap);
        let updated_statuses = self.find_updated_statuses(nodemap);
        self.update_status(nodemap, updated_statuses);
    }

    /// Set the number of running jobs.
    fn set_number_of_running_jobs(&mut self, nodemap: &HashMap<String, Node>) {
        let job_ids = self.backend.get_running_job_ids(nodemap);
        self.nrunning = job_ids.len();
    }

    /// Find nodes ready for submission.
    fn find_updated_statuses(
        &mut self,
        nodemap: &HashMap<String, Node>,
    ) -> HashMap<String, JobStatus> {
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
            self.update_try_count(updated_node);
        }
    }

    /// Update the retry count for rescheduled tasks.
    fn update_try_count(&self, node: &mut Node) {
        if let NodeBehavior::TaskNode(task) = &mut node.behavior {
            match node.status {
                JobStatus::Running(_) | JobStatus::Failed(_) => task.try_num += 1,
                _ => {}
            }
        }
    }

    /// Submit a node based on its behavior.
    fn submit_node(&mut self, uid: &str, nodemap: &HashMap<String, Node>) -> JobStatus {
        let node = nodemap.get(uid).unwrap();
        match &node.behavior {
            NodeBehavior::RootNode { .. } | NodeBehavior::EndNode { .. } => {
                JobStatus::Completed(NodeResult::Node)
            }
            NodeBehavior::TaskNode(task) => {
                if self.max_concurrency > 0 && self.nrunning >= self.max_concurrency {
                    return JobStatus::NotSubmitted;
                }
                self.submit_tasknode(&task, &node.uid)
            }
            NodeBehavior::OneOfNode { .. } => match self.submit_oneofnode(&node.uid, &nodemap) {
                Ok(uid) => JobStatus::Completed(NodeResult::OneOf(uid)),
                Err(_) => JobStatus::Failed(NodeFailure::Node),
            },
            NodeBehavior::IfNode { .. } => match self.submit_ifnode(&node.uid, &nodemap) {
                Err(_) => JobStatus::Failed(NodeFailure::Node),
                Ok(branch) => JobStatus::Completed(NodeResult::If(branch)),
            },
        }
    }

    /// Submit a task.
    fn submit_tasknode(&mut self, task: &Task, uid: &str) -> JobStatus {
        log::debug!("Submitting task '{}' of node '{}'", task.fname, uid);
        let pipeline_dir = self.state.get_pipeline_dir();
        let res = self.backend.submit(
            &task.launch_script,
            &uid,
            &task.fname,
            pipeline_dir,
            &task.try_num,
        );
        match res {
            Ok(job_id) => {
                self.nrunning += 1;
                JobStatus::Running(job_id)
            }
            Err(e) => {
                log::error!("Failed task {uid} submission: {e}");
                JobStatus::Failed(NodeFailure::Node)
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
    use crate::model::{DAG, ExecMode, Parent, ParentType};

    use super::*;
    use std::path::PathBuf;

    /// Mocked backend for testing purposes.
    struct MockBackend;
    impl Backend for MockBackend {
        fn update_status(&self, _nodemap: &mut HashMap<String, Node>, _state: &impl StateManager) {}
        fn submit(
            &self,
            _launch_script: &str,
            _uid: &str,
            _fname: &str,
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
        fn prepare(&self, _dag: &DAG<Node>) -> io::Result<()> {
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
        fn read_checkpoint(&self) -> io::Result<String> {
            Ok(String::from("checkpoint"))
        }

        fn save_checkpoint(&self, _dag: &DAG<&Node>) -> io::Result<()> {
            Ok(())
        }

        fn validate_checkpoint(&self, _dag: &DAG<Node>) -> Result<(), String> {
            Ok(())
        }

        fn cache_task(&self, _fname: &str, _uid: &str) -> io::Result<u64> {
            Ok(1)
        }

        fn copy_cache(&self, _fname: &str, _uid: &str) -> io::Result<u64> {
            Ok(1)
        }
    }

    /// Find the completed parent uid for the OneOf node submission.
    #[test]
    fn find_parent_uid_for_oneof() {
        let state = MockState::new();
        let submitter = Submitter::new(&MockBackend, &state, 0);
        let parent_statuses = HashMap::from([
            (String::from("a"), JobStatus::NotSubmitted),
            (String::from("b"), JobStatus::Completed(NodeResult::Node)),
            (String::from("c"), JobStatus::Failed(NodeFailure::Node)),
        ]);

        let uid = submitter.find_completed_parent_uid(&parent_statuses);
        assert_eq!(uid, String::from("b"));
    }

    /// A rootnode is just marked as completed.
    #[test]
    fn submit_root() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
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

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    /// And endnode is just marked as completed.
    #[test]
    fn submit_end() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
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

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    /// If successful, the result of the IfNode contains the selected branch.
    #[test]
    fn submit_if() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);

        assert!(matches!(
            new_status,
            JobStatus::Completed(NodeResult::If(true))
        ));
    }

    /// Submit a task with the help of the backend.
    #[test]
    fn submit_task() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: false,
                mode: ExecMode::Wrap,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            }),
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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);

        assert!(matches!(new_status, JobStatus::Running(_)));
        assert_eq!(submitter.nrunning, 1);
    }

    /// Submit a OneOf node.
    #[test]
    fn submit_oneof() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Failed(NodeFailure::Node),
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let p2 = Node {
            uid: String::from("p2"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);

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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let state = MockState::new();
        let submitter = Submitter::new(&MockBackend, &state, 0);

        let mut nodemap = HashMap::from([("c".to_string(), node)]);
        let updated_statuses =
            HashMap::from([("c".to_string(), JobStatus::Failed(NodeFailure::Node))]);

        submitter.update_status(&mut nodemap, updated_statuses);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::Failed(NodeFailure::Node)))
    }

    /// Update the status of a task.
    #[test]
    fn update_task_status() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: false,
                mode: ExecMode::Wrap,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            }),
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let state = MockState::new();
        let submitter = Submitter::new(&MockBackend, &state, 0);

        let mut nodemap = HashMap::from([("c".to_string(), node)]);
        let updated_statuses =
            HashMap::from([("c".to_string(), JobStatus::Failed(NodeFailure::Node))]);

        submitter.update_status(&mut nodemap, updated_statuses);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::Failed(NodeFailure::Node)));
        assert!(matches!(
            child.behavior,
            NodeBehavior::TaskNode(ref task) if task.try_num == 1
        ));
    }

    /// Task not submitted, the try count is not updated.
    #[test]
    fn update_nonsubmitted_task_status() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: false,
                mode: ExecMode::Wrap,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            }),
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let state = MockState::new();
        let submitter = Submitter::new(&MockBackend, &state, 0);

        let mut nodemap = HashMap::from([("c".to_string(), node)]);
        let updated_statuses = HashMap::from([("c".to_string(), JobStatus::NotSubmitted)]);

        submitter.update_status(&mut nodemap, updated_statuses);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::NotSubmitted));
        assert!(matches!(
            child.behavior,
            NodeBehavior::TaskNode(ref task) if task.try_num == 0
        ));
    }

    /// Complete graph execution test.
    #[test]
    fn e2e() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: Vec::new(),
        };

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 0);
        let mut nodemap = HashMap::from([("c".to_string(), node)]);

        submitter.submit(&mut nodemap);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(
            child.status,
            JobStatus::Completed(NodeResult::Node)
        ))
    }

    /// Verify the number of running tasks is correctly set.
    #[test]
    fn check_number_of_running_tasks() {
        let nodemap = HashMap::from([
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::Running(String::from("1234")),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("function"),
                        launch_script: String::from("script"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        retries: 0,
                        try_num: 0,
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
            (
                String::from("2"),
                Node {
                    uid: String::from("2"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("function"),
                        launch_script: String::from("script"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        retries: 0,
                        try_num: 0,
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
        ]);

        let state = MockState::new();
        let mut submitter = Submitter::new(&MockBackend, &state, 2);
        submitter.set_number_of_running_jobs(&nodemap);
        assert_eq!(submitter.nrunning, 1);
    }

    // Task not submitted because the max concurrency is already reached.
    #[test]
    fn max_concurrency_reached() {
        let node = Node {
            uid: String::from("c"),
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("function"),
                launch_script: String::from("script"),
                caching: false,
                mode: ExecMode::Wrap,
                retries: 0,
                try_num: 0,
                input_kwargs: Vec::new(),
            }),
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
            output_artifacts: Vec::new(),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
            children: vec!["c".to_string()],
        };

        let state = MockState::new();
        let mut submitter = Submitter {
            backend: &MockBackend,
            state: &state,
            max_concurrency: 1,
            nrunning: 1,
        };

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::NotSubmitted));
        assert_eq!(submitter.nrunning, 1);
    }
}
