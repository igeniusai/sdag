use crate::backend::Backend;
use crate::model::{BooleanOutput, JobStatus, Node, NodeBehavior, NodeResult};
use crate::state::StateManager;
use crate::status_management::StatusSelector;
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::io;
#[derive(Debug, Clone)]
pub struct Submitter<'a, T: Backend, U: StateManager> {
    pub backend: &'a T,
    pub state: &'a U,
}

impl<'a, T: Backend, U: StateManager> Submitter<'a, T, U> {
    pub fn submit(&self, nodemap: &mut HashMap<String, Node>) {
        let updated_statuses = self.find_updated_statuses(nodemap);
        self.update_status(nodemap, updated_statuses);
    }

    fn find_updated_statuses(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, JobStatus> {
        nodemap
            .iter()
            .filter(|(_, node)| matches!(node.status, JobStatus::ReadyForSubmission))
            .map(|(k, node)| (k.clone(), self.submit_node(node, nodemap)))
            .collect()
    }

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

    fn submit_node(&self, node: &Node, nodemap: &HashMap<String, Node>) -> JobStatus {
        match &node.behavior {
            NodeBehavior::RootNode { .. } | NodeBehavior::EndNode { .. } => {
                JobStatus::Completed(NodeResult::Node)
            }
            NodeBehavior::TaskNode { launch_script, .. } => {
                match self.submit_tasknode(&node.uid, launch_script) {
                    Ok(job_id) => JobStatus::Running(job_id),
                    Err(_) => JobStatus::Failed,
                }
            }
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

    fn submit_tasknode(&self, uid: &str, launch_script: &str) -> Result<String, Box<dyn Error>> {
        let pipeline_dir = self.state.get_pipeline_dir();
        self.backend.submit(launch_script, uid, pipeline_dir)
    }

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

    fn submit_oneofnode(&self, uid: &str, nodemap: &HashMap<String, Node>) -> io::Result<String> {
        let parent_statuses = StatusSelector::get_parent_statuses(uid, nodemap);
        let parent_uid = self.find_completed_parent_uid(&parent_statuses);
        self.state.copy_output(&parent_uid, uid)?;
        Ok(parent_uid)
    }

    fn find_completed_parent_uid(&self, parent_statuses: &HashMap<String, JobStatus>) -> String {
        parent_statuses
            .iter()
            .filter(|x| matches!(x.1, JobStatus::Completed { .. }))
            .map(|x| x.0.to_string())
            .next()
            .unwrap()
    }
}

//#[cfg(test)]
//mod tests {
//    use crate::model::Parent;
//
//    use super::*;
//    use std::path::PathBuf;
//
//    struct MockBackend;
//    impl Backend for MockBackend {
//        fn update_status(&self, _nodemap: &mut HashMap<String, Node>) {}
//        fn submit(&self, _launch_script: &str, _uid: &str, _pipeline_dir: &str) -> String {
//            String::from("submitted")
//        }
//    }
//
//    struct MockState {
//        fake_path: PathBuf,
//    }
//    impl MockState {
//        fn new() -> Self {
//            Self {
//                fake_path: PathBuf::from("path"),
//            }
//        }
//    }
//
//    impl StateManager for MockState {
//        fn prepare(&self, _dag: &crate::model::DAG) {}
//        fn copy_output(&self, _src_uid: &str, _dst_uid: &str) -> std::io::Result<u64> {
//            std::io::Result::Ok(1)
//        }
//        fn read_output(&self, _uid: &str) -> std::io::Result<String> {
//            std::io::Result::Ok(String::from(r#"{"output":true}"#))
//        }
//        fn get_pipeline_dir(&self) -> &PathBuf {
//            &self.fake_path
//        }
//    }
//
//    fn get_submitter() -> Submitter<MockBackend, MockState> {
//        Submitter {
//            backend: MockBackend,
//            state: MockState::new(),
//        }
//    }
//
//    #[test]
//    fn find_parent_uid_for_oneof() {
//        let submitter = get_submitter();
//
//        let parent_statuses = HashMap::from([
//            (String::from("a"), JobStatus::NotSubmitted),
//            (String::from("b"), JobStatus::Completed),
//            (String::from("c"), JobStatus::Failed),
//        ]);
//
//        let uid = submitter.find_completed_parent_uid(&parent_statuses);
//        assert_eq!(uid, String::from("b"));
//    }
//
//    #[test]
//    fn submit_root() {
//        let mut node = Node {
//            uid: String::from("c"),
//            behavior: NodeBehavior::RootNode {
//                children: Vec::new(),
//            },
//            status: JobStatus::ReadyForSubmission,
//            parents: vec![Parent {
//                name: String::from("p"),
//                uid: String::from("p"),
//            }],
//        };
//
//        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
//        let submitter = get_submitter();
//        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
//        assert!(matches!(node.status, JobStatus::Completed))
//    }
//
//    #[test]
//    fn submit_end() {
//        let mut node = Node {
//            uid: String::from("c"),
//            behavior: NodeBehavior::EndNode {
//                children: Vec::new(),
//            },
//            status: JobStatus::ReadyForSubmission,
//            parents: vec![Parent {
//                name: String::from("p"),
//                uid: String::from("p"),
//            }],
//        };
//
//        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
//        let submitter = get_submitter();
//        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
//        assert!(matches!(node.status, JobStatus::Completed))
//    }
//
//    #[test]
//    fn submit_if() {
//        let mut node = Node {
//            uid: String::from("c"),
//            behavior: NodeBehavior::IfNode {
//                selected: None,
//                true_branch: Vec::new(),
//                false_branch: Vec::new(),
//            },
//            status: JobStatus::ReadyForSubmission,
//            parents: vec![Parent {
//                name: String::from("p"),
//                uid: String::from("p"),
//            }],
//        };
//
//        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
//        let submitter = get_submitter();
//        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
//        assert!(matches!(node.status, JobStatus::Completed));
//    }
//
//    #[test]
//    fn submit_task() {
//        let mut node = Node {
//            uid: String::from("c"),
//            behavior: NodeBehavior::TaskNode {
//                fname: String::from("fname"),
//                launch_script: String::from("submit.sh"),
//                return_type: None,
//                children: Vec::new(),
//            },
//            status: JobStatus::ReadyForSubmission,
//            parents: vec![Parent {
//                name: String::from("p"),
//                uid: String::from("p"),
//            }],
//        };
//
//        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
//        let submitter = get_submitter();
//        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
//        assert!(matches!(node.status, JobStatus::Running(_)));
//    }
//
//    #[test]
//    fn submit_oneof() {
//        let mut node = Node {
//            uid: String::from("c"),
//            behavior: NodeBehavior::OneOfNode {
//                children: Vec::new(),
//            },
//            status: JobStatus::ReadyForSubmission,
//            parents: vec![Parent {
//                name: String::from("p"),
//                uid: String::from("p"),
//            }],
//        };
//
//        let parent_statuses = HashMap::from([(String::from("c"), JobStatus::Completed)]);
//        let submitter = get_submitter();
//        submitter.submit_based_on_behavior(&mut node, &parent_statuses);
//        assert!(matches!(node.status, JobStatus::Completed));
//    }
//}
