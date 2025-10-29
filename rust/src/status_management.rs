use crate::backend::{Backend, SlurmBackend};
use crate::model::{JobStatus, Node, NodeBehavior};
use std::collections::HashMap;

pub fn update_status(uid: &str, nodemap: &mut HashMap<String, Node>, backend: &SlurmBackend) {
    backend.update_status(nodemap);
    recoursively_update_status(uid, nodemap);
}

fn recoursively_update_status(uid: &str, nodemap: &mut HashMap<String, Node>) {
    let manager = ParentStatusManager::new(uid, &nodemap);
    if let Some(node) = nodemap.get_mut(uid) {
        if let JobStatus::NotSubmitted = node.status {
            node.status = manager.get_status_from_parents(&node.behavior);
        }

        for child_uid in &node.get_all_children() {
            recoursively_update_status(child_uid, nodemap);
        }
    }
}

pub struct ParentStatusManager {
    parent_statuses: Vec<JobStatus>,
}
impl ParentStatusManager {
    pub fn new(uid: &str, nodemap: &HashMap<String, Node>) -> Self {
        let status_map = Self::get_parent_statuses(uid, nodemap);
        let parent_statuses = status_map.into_values().collect();
        Self { parent_statuses }
    }

    pub fn get_parent_statuses(
        uid: &str,
        nodemap: &HashMap<String, Node>,
    ) -> HashMap<String, JobStatus> {
        let node = nodemap.get(uid).unwrap();
        node.parents
            .iter()
            .map(|p| nodemap.get(&p.uid).unwrap())
            .map(|p| (p.uid.to_string(), p.get_status_for_child(uid)))
            .collect()
    }

    fn get_status_from_parents(&self, node_behavior: &NodeBehavior) -> JobStatus {
        match node_behavior {
            NodeBehavior::OneOfNode { .. } => self.get_oneof_status(),
            _ => self.get_default_status(),
        }
    }

    fn get_default_status(&self) -> JobStatus {
        if self.all_parents_completed() {
            JobStatus::ReadyForSubmission
        } else if self.some_parents_failed() {
            JobStatus::Failed
        } else if self.some_parents_skipped() {
            JobStatus::Skipped
        } else {
            JobStatus::NotSubmitted
        }
    }

    fn get_oneof_status(&self) -> JobStatus {
        if self.some_parents_completed() {
            JobStatus::ReadyForSubmission
        } else if self.all_parents_skipped() {
            JobStatus::Skipped
        } else if self.all_parents_failed_or_skipped() {
            JobStatus::Failed
        } else {
            JobStatus::NotSubmitted
        }
    }

    fn all_parents_completed(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Completed))
    }

    fn some_parents_completed(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Completed))
    }

    fn some_parents_failed(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Failed))
    }

    fn all_parents_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Skipped))
    }

    fn some_parents_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Skipped))
    }

    fn all_parents_failed_or_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Failed) | matches!(x, JobStatus::Skipped))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::model::Parent;

    macro_rules! oneof_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (parent_statuses, _expected) = $value;
                    let manager = ParentStatusManager {
                        parent_statuses,
                    };
                    assert!(matches!(manager.get_oneof_status(), _expected));
                }
            )*
        }
    }

    oneof_tests! {
        oneof_test_0: (vec![JobStatus::Completed, JobStatus::Completed], JobStatus::ReadyForSubmission),
        oneof_test_1: (vec![JobStatus::Failed, JobStatus::Completed], JobStatus::ReadyForSubmission),
        oneof_test_2: (vec![JobStatus::Skipped, JobStatus::Completed], JobStatus::ReadyForSubmission),
        oneof_test_3: (vec![JobStatus::Running(String::from("..")), JobStatus::Completed], JobStatus::ReadyForSubmission),
        oneof_test_4: (vec![JobStatus::Failed, JobStatus::Failed], JobStatus::Failed),
        oneof_test_5: (vec![JobStatus::Running(String::from("..")), JobStatus::NotSubmitted], JobStatus::NotSubmitted),
        oneof_test_6: (vec![JobStatus::Skipped, JobStatus::Failed], JobStatus::Failed),
        oneof_test_7: (vec![JobStatus::Skipped, JobStatus::Skipped], JobStatus::Skipped),
    }

    macro_rules! default_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (parent_statuses, _expected) = $value;
                    let manager = ParentStatusManager {
                        parent_statuses,
                    };
                    assert!(matches!(manager.get_default_status(), _expected));
                }
            )*
        }
    }

    default_tests! {
        node_test_0: (vec![JobStatus::Completed, JobStatus::Completed], JobStatus::ReadyForSubmission),
        node_test_1: (vec![JobStatus::Failed, JobStatus::Completed], JobStatus::Failed),
        node_test_2: (vec![JobStatus::Skipped, JobStatus::Completed], JobStatus::Failed),
        node_test_3: (vec![JobStatus::Running(String::from("..")), JobStatus::Completed], JobStatus::NotSubmitted),
        node_test_4: (vec![JobStatus::Failed, JobStatus::Failed], JobStatus::Failed),
        node_test_5: (vec![JobStatus::Running(String::from("..")), JobStatus::NotSubmitted], JobStatus::NotSubmitted),
        node_test_6: (vec![JobStatus::Skipped, JobStatus::Failed], JobStatus::Failed),
        node_test_7: (vec![JobStatus::Skipped, JobStatus::Skipped], JobStatus::Skipped),
    }

    #[test]
    fn get_oneof_status_from_parents() {
        let manager = ParentStatusManager {
            parent_statuses: vec![JobStatus::Failed, JobStatus::Completed],
        };
        let node_behavior = NodeBehavior::OneOfNode {
            children: Vec::new(),
        };
        matches!(
            manager.get_status_from_parents(&node_behavior),
            JobStatus::ReadyForSubmission
        );
    }

    #[test]
    fn get_default_status_from_parents() {
        let manager = ParentStatusManager {
            parent_statuses: vec![JobStatus::Failed, JobStatus::Completed],
        };
        let node_behavior = NodeBehavior::RootNode {
            children: Vec::new(),
        };
        matches!(
            manager.get_status_from_parents(&node_behavior),
            JobStatus::Failed
        );
    }

    #[test]
    fn manager_creation() {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p1"),
            Node {
                uid: String::from("p1"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
            },
        );
        nodemap.insert(
            String::from("p2"),
            Node {
                uid: String::from("p2"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::Completed,
                parents: Vec::new(),
            },
        );

        nodemap.insert(
            String::from("c"),
            Node {
                uid: String::from("c"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::NotSubmitted,
                parents: vec![Parent {
                    name: String::from("p2"),
                    uid: String::from("p2"),
                }],
            },
        );

        let manager = ParentStatusManager::new("c", &nodemap);
        assert_eq!(manager.parent_statuses.len(), 1);
        assert!(matches!(manager.parent_statuses[0], JobStatus::Completed));
    }

    #[test]
    fn check_recoursive_update() {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p"),
            Node {
                uid: String::from("p"),
                behavior: NodeBehavior::RootNode {
                    children: vec![String::from("c")],
                },
                status: JobStatus::Completed,
                parents: Vec::new(),
            },
        );

        nodemap.insert(
            String::from("c"),
            Node {
                uid: String::from("c"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::NotSubmitted,
                parents: vec![Parent {
                    name: String::from("p"),
                    uid: String::from("p"),
                }],
            },
        );

        recoursively_update_status("p", &mut nodemap);
        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::ReadyForSubmission))
    }
}
