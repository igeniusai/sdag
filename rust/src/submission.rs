use crate::backend::Backend;
use crate::model::{BooleanOutput, JobStatus, Node, NodeBehavior};
use crate::state::StateManager;
use crate::status_management::ParentStatusManager;
use serde_json;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Submitter<T: Backend, U: StateManager> {
    pub backend: T,
    pub state: U,
}

impl<T: Backend, U: StateManager> Submitter<T, U> {
    pub fn submit(&self, uid: &str, nodemap: &mut HashMap<String, Node>) {
        let node = nodemap.get(uid).unwrap();
        let child_uids = node.get_all_children();
        self.submit_and_update_status(uid, nodemap);

        for child_uid in &child_uids {
            self.submit(child_uid, nodemap);
        }
    }

    fn submit_and_update_status(&self, uid: &str, nodemap: &mut HashMap<String, Node>) {
        let parent_statuses = ParentStatusManager::get_parent_statuses(uid, nodemap);
        let node = nodemap.get_mut(uid).unwrap();

        if let JobStatus::ReadyForSubmission = node.status {
            self.submit_based_on_behavior(node, &parent_statuses);
        }
    }

    fn submit_based_on_behavior(
        &self,
        node: &mut Node,
        parent_statuses: &HashMap<String, JobStatus>,
    ) {
        match &mut node.behavior {
            NodeBehavior::RootNode { .. } | NodeBehavior::EndNode { .. } => {
                node.status = JobStatus::Completed;
            }
            NodeBehavior::IfNode { selected, .. } => {
                let branch = self.submit_ifnode(&parent_statuses);
                *selected = Some(branch);
                node.status = JobStatus::Completed
            }
            NodeBehavior::TaskNode { launch_script, .. } => {
                node.status = self.submit_tasknode(&node.uid, launch_script);
            }
            NodeBehavior::OneOfNode { .. } => {
                node.status = self.submit_oneofnode(&node.uid, &parent_statuses);
            }
        }
    }

    fn submit_tasknode(&self, uid: &str, launch_script: &str) -> JobStatus {
        let pipeline_dir = self.state.get_pipeline_dir().to_str().unwrap();
        let job_id = self.backend.submit(launch_script, uid, pipeline_dir);
        JobStatus::Running(job_id)
    }

    fn submit_ifnode(&self, parent_statuses: &HashMap<String, JobStatus>) -> bool {
        let parent_uid = &self.find_completed_parent_uid(parent_statuses);
        let output = self.state.read_output(parent_uid);
        match output {
            Ok(data) => {
                let choice: BooleanOutput = serde_json::from_str(&data).unwrap();
                choice.output
            }
            Err(_) => panic!("Failed to read if node condition"),
        }
    }

    fn submit_oneofnode(
        &self,
        uid: &str,
        parent_statuses: &HashMap<String, JobStatus>,
    ) -> JobStatus {
        let parent_uid = self.find_completed_parent_uid(&parent_statuses);
        let res = self.state.copy_output(&parent_uid, uid);
        match res {
            Ok(..) => JobStatus::Completed,
            Err(..) => JobStatus::Failed,
        }
    }

    fn find_completed_parent_uid(&self, parent_statuses: &HashMap<String, JobStatus>) -> String {
        parent_statuses
            .iter()
            .filter(|x| matches!(x.1, JobStatus::Completed))
            .map(|x| x.0.to_string())
            .next()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::model::Parent;

    use super::*;
    use std::path::PathBuf;

    struct MockBackend;
    impl Backend for MockBackend {
        fn update_status(&self, _nodemap: &mut HashMap<String, Node>) {}
        fn submit(&self, _launch_script: &str, _uid: &str, _pipeline_dir: &str) -> String {
            String::from("submitted")
        }
    }

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
        fn prepare(&self, _dag: &crate::model::DAG) {}
        fn copy_output(&self, _src_uid: &str, _dst_uid: &str) -> std::io::Result<u64> {
            std::io::Result::Ok(1)
        }
        fn read_output(&self, _uid: &str) -> std::io::Result<String> {
            std::io::Result::Ok(String::from(r#"{"output":true}"#))
        }
        fn get_pipeline_dir(&self) -> &PathBuf {
            &self.fake_path
        }
    }

    fn get_submitter() -> Submitter<MockBackend, MockState> {
        Submitter {
            backend: MockBackend,
            state: MockState::new(),
        }
    }

    #[test]
    fn find_parent_uid_for_oneof() {
        let submitter = get_submitter();

        let parent_statuses = HashMap::from([
            (String::from("a"), JobStatus::NotSubmitted),
            (String::from("b"), JobStatus::Completed),
            (String::from("c"), JobStatus::Failed),
        ]);

        let uid = submitter.find_completed_parent_uid(&parent_statuses);
        assert_eq!(uid, String::from("b"));
    }

    #[test]
    fn submit_root() {
        let mut node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
        let submitter = get_submitter();
        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
        assert!(matches!(node.status, JobStatus::Completed))
    }

    #[test]
    fn submit_end() {
        let mut node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::EndNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
        let submitter = get_submitter();
        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
        assert!(matches!(node.status, JobStatus::Completed))
    }

    #[test]
    fn submit_if() {
        let mut node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::IfNode {
                selected: None,
                true_branch: Vec::new(),
                false_branch: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
        let submitter = get_submitter();
        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
        assert!(matches!(node.status, JobStatus::Completed));
    }

    #[test]
    fn submit_task() {
        let mut node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                launch_script: String::from("submit.sh"),
                return_type: None,
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
        let submitter = get_submitter();
        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
        assert!(matches!(node.status, JobStatus::Running(_)));
    }

    #[test]
    fn submit_oneof() {
        let mut node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::OneOfNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
        let submitter = get_submitter();
        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
        assert!(matches!(node.status, JobStatus::Completed));
    }
}
