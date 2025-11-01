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
            .map(|(k, _)| (k.clone(), self.submit_node(k, nodemap)))
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
                ..
            } => self.submit_tasknode(&node.uid, launch_script, caching, try_num),
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

    fn submit_tasknode(
        &self,
        uid: &str,
        launch_script: &str,
        caching: &bool,
        try_num: &u32,
    ) -> JobStatus {
        if *caching && self.state.is_task_cached(uid) {
            log::info!("Task {uid} is cached");
            return JobStatus::Completed(NodeResult::Node);
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

#[cfg(test)]
mod tests {
    use crate::model::{DAG, Parent};

    use super::*;
    use std::path::PathBuf;

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

        fn is_task_cached(&self, uid: &str) -> bool {
            uid == "_cached_"
        }
    }

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

    #[test]
    fn submit_root() {
        let node = Node {
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

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    #[test]
    fn submit_end() {
        let node = Node {
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

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    #[test]
    fn submit_if() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::IfNode {
                true_branch: Vec::new(),
                false_branch: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode {
                children: vec!["c".to_string()],
            },
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
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

    #[test]
    fn submit_task() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("function"),
                launch_script: String::from("script"),
                return_type: None,
                caching: false,
                retries: 0,
                try_num: 0,
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode {
                children: vec!["c".to_string()],
            },
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("c".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("c", &nodemap);
        assert!(matches!(new_status, JobStatus::Running(_)))
    }

    #[test]
    fn submit_cached_task() {
        let node = Node {
            uid: String::from("_cached_"),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("function"),
                launch_script: String::from("script"),
                return_type: None,
                caching: true,
                retries: 0,
                try_num: 0,
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![Parent {
                name: String::from("p"),
                uid: String::from("p"),
            }],
        };

        let parent = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode {
                children: vec!["c".to_string()],
            },
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
        };

        let submitter = Submitter {
            backend: &MockBackend,
            state: &MockState::new(),
        };

        let nodemap = HashMap::from([("_cached_".to_string(), node), ("p".to_string(), parent)]);
        let new_status = submitter.submit_node("_cached_", &nodemap);
        assert!(matches!(new_status, JobStatus::Completed(NodeResult::Node)))
    }

    #[test]
    fn submit_oneof() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::OneOfNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: vec![
                Parent {
                    name: String::from("p1"),
                    uid: String::from("p1"),
                },
                Parent {
                    name: String::from("p2"),
                    uid: String::from("p2"),
                },
            ],
        };

        let p1 = Node {
            uid: String::from("p1"),
            behavior: NodeBehavior::RootNode {
                children: vec!["c".to_string()],
            },
            status: JobStatus::Failed,
            parents: Vec::new(),
        };

        let p2 = Node {
            uid: String::from("p2"),
            behavior: NodeBehavior::RootNode {
                children: vec!["c".to_string()],
            },
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
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

    #[test]
    fn find_updated_statuses() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
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

    #[test]
    fn update_status() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
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

    #[test]
    fn e2e() {
        let node = Node {
            uid: String::from("c"),
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::ReadyForSubmission,
            parents: Vec::new(),
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
