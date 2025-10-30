use crate::backend::{Backend, SlurmBackend};
use crate::model::{JobStatus, Node, NodeBehavior};
use std::collections::HashMap;

pub fn update_status(uid: &str, nodemap: &mut HashMap<String, Node>, backend: &SlurmBackend) {
    backend.update_status(nodemap);
    let mut updated_statuses: HashMap<String, JobStatus> = HashMap::new();
    recoursively_update_status(uid, nodemap, &mut updated_statuses);
    for (k, v) in updated_statuses.into_iter() {
        let updated_node = nodemap.get_mut(&k).unwrap();
        updated_node.status = v;
    }
}

fn recoursively_update_status(
    uid: &str,
    nodemap: &HashMap<String, Node>,
    updated_statuses: &mut HashMap<String, JobStatus>,
) -> Option<()> {
    let node = nodemap.get(uid)?;
    if let JobStatus::NotSubmitted = node.status {
        let selector = StatusSelector::new(uid, nodemap, updated_statuses);
        let updated_status = selector.get_updated_status(&node.behavior);
        updated_statuses.insert(node.uid.clone(), updated_status);
    }

    for child_uid in node.get_all_children() {
        recoursively_update_status(&child_uid, nodemap, updated_statuses);
    }

    Some(())
}

pub struct StatusSelector {
    parent_statuses: Vec<JobStatus>,
}
impl StatusSelector {
    pub fn new(
        uid: &str,
        nodemap: &HashMap<String, Node>,
        updated_statuses: &HashMap<String, JobStatus>,
    ) -> Self {
        let mut status_map = Self::get_parent_statuses(uid, nodemap);
        for (k, v) in updated_statuses.iter() {
            if let Some(value) = status_map.get_mut(k) {
                *value = v.clone();
            }
        }

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

    fn get_updated_status(&self, node_behavior: &NodeBehavior) -> JobStatus {
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
            .all(|x| matches!(x, JobStatus::Completed { .. }))
    }

    fn some_parents_completed(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Completed { .. }))
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
    use crate::model::{NodeResult, Parent};

    macro_rules! oneof_tests {
            ($($name:ident: $value:expr,)*) => {
                $(
                    #[test]
                    fn $name() {
                        let (parent_statuses, _expected) = $value;
                        let selector = StatusSelector {
                            parent_statuses,
                        };
                        assert!(matches!(selector.get_oneof_status(), _expected));
                    }
                )*
            }
        }

    oneof_tests! {
        oneof_test_0: (
            vec![
                JobStatus::Completed(NodeResult::Node),
                JobStatus::Completed(NodeResult::Node),
            ],
            JobStatus::ReadyForSubmission
        ),
        oneof_test_1: (
            vec![
                JobStatus::Failed,
                JobStatus::Completed(NodeResult::Node),
            ],
            JobStatus::ReadyForSubmission
        ),
        oneof_test_2: (
            vec![
                JobStatus::Skipped,
                JobStatus::Completed(NodeResult::OneOf("0".to_string())),
            ],
            JobStatus::ReadyForSubmission
        ),
        oneof_test_3: (
            vec![
                JobStatus::Running("..".to_string()),
                JobStatus::Completed(NodeResult::If(true)),
            ],
            JobStatus::ReadyForSubmission
        ),
        oneof_test_4: (
            vec![
                JobStatus::Failed,
                JobStatus::Failed
            ],
            JobStatus::Failed
        ),
        oneof_test_5: (
            vec![
                JobStatus::Running(String::from("..")),
                JobStatus::NotSubmitted
            ],
            JobStatus::NotSubmitted
        ),
        oneof_test_6: (
            vec![
                JobStatus::Skipped,
                JobStatus::Failed
            ],
            JobStatus::Failed
        ),
        oneof_test_7: (
            vec![
                JobStatus::Skipped,
                JobStatus::Skipped
            ],
            JobStatus::Skipped
        ),
    }

    macro_rules! default_tests {
            ($($name:ident: $value:expr,)*) => {
                $(
                    #[test]
                    fn $name() {
                        let (parent_statuses, _expected) = $value;
                        let selector = StatusSelector {
                            parent_statuses,
                        };
                        assert!(matches!(selector.get_default_status(), _expected));
                    }
                )*
            }
        }

    default_tests! {
        node_test_0: (
            vec![
                JobStatus::Completed(NodeResult::Node),
                JobStatus::Completed(NodeResult::Node),
            ],
            JobStatus::ReadyForSubmission),
        node_test_1: (
            vec![
                JobStatus::Failed,
                JobStatus::Completed(NodeResult::OneOf("0".to_string())),
            ],
            JobStatus::Failed,
        ),
        node_test_2: (
            vec![
                JobStatus::Skipped,
                JobStatus::Completed(NodeResult::Node),
            ],
            JobStatus::Failed),
        node_test_3: (
            vec![
                JobStatus::Running(String::from("..")),
                JobStatus::Completed(NodeResult::If(false)),
            ],
            JobStatus::NotSubmitted,
        ),
        node_test_4: (
            vec![
                JobStatus::Failed,
                JobStatus::Failed,
            ],
            JobStatus::Failed
        ),
        node_test_5: (
            vec![
                JobStatus::Running(String::from("..")),
                JobStatus::NotSubmitted,
            ],
            JobStatus::NotSubmitted
        ),
        node_test_6: (
            vec![
                JobStatus::Skipped,
                JobStatus::Failed
            ],
            JobStatus::Failed
        ),
        node_test_7: (
            vec![
                JobStatus::Skipped,
                JobStatus::Skipped
            ],
            JobStatus::Skipped
        ),
    }

    #[test]
    fn get_oneof_updated_status() {
        let selector = StatusSelector {
            parent_statuses: vec![JobStatus::Failed, JobStatus::Completed(NodeResult::Node)],
        };
        let node_behavior = NodeBehavior::OneOfNode {
            children: Vec::new(),
        };
        matches!(
            selector.get_updated_status(&node_behavior),
            JobStatus::ReadyForSubmission
        );
    }

    #[test]
    fn get_default_updated_status() {
        let selector = StatusSelector {
            parent_statuses: vec![
                JobStatus::Failed,
                JobStatus::Completed(NodeResult::If(true)),
            ],
        };
        let node_behavior = NodeBehavior::RootNode {
            children: Vec::new(),
        };
        matches!(
            selector.get_updated_status(&node_behavior),
            JobStatus::Failed
        );
    }

    fn get_test_nodemap() -> HashMap<String, Node> {
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
                status: JobStatus::NotSubmitted,
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
        nodemap
    }

    #[test]
    fn get_parent_statuses() {
        let nodemap = get_test_nodemap();
        let parent_statuses = StatusSelector::get_parent_statuses("c", &nodemap);
        assert_eq!(
            parent_statuses,
            HashMap::from([("p2".to_string(), JobStatus::NotSubmitted)])
        );
    }

    #[test]
    fn get_selector_creation() {
        let nodemap = get_test_nodemap();
        let updated_statuses = HashMap::from([("p2".to_string(), JobStatus::Failed)]);
        let selector = StatusSelector::new("c", &nodemap, &updated_statuses);
        assert_eq!(selector.parent_statuses, vec![JobStatus::Failed]);
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
                status: JobStatus::Completed(NodeResult::Node),
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

        let mut updated_statuses = HashMap::new();
        recoursively_update_status("p", &nodemap, &mut updated_statuses);
        let child_status = updated_statuses.get("c").unwrap();
        assert!(matches!(child_status, JobStatus::ReadyForSubmission))
    }

    #[test]
    fn e2e() {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p"),
            Node {
                uid: String::from("p"),
                behavior: NodeBehavior::RootNode {
                    children: vec![String::from("c")],
                },
                status: JobStatus::Completed(NodeResult::Node),
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

        let backend = SlurmBackend;
        update_status("p", &mut nodemap, &backend);

        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::ReadyForSubmission))
    }
}
