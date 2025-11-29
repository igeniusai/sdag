//! Recursive status update and retry management.

use crate::backend::{Backend, SlurmBackend};
use crate::model::{JobStatus, Node, NodeBehavior};
use log;
use std::collections::HashMap;

/// Reschedule a failed job if possible.
///
/// To be rescheduled, a node:
/// - Must be a task
/// - The number of retries must be lower than the user-selected ones
fn set_retry_if_possible(node: &mut Node) {
    if let NodeBehavior::TaskNode {
        try_num, retries, ..
    } = &mut node.behavior
    {
        if *try_num > 0 && try_num <= retries {
            log::info!("Node {} scheduled for resubmission", node.uid);
            node.status = JobStatus::ReadyForSubmission;
        }
    }
}

/// Manage retries
fn schedule_retries(nodemap: &mut HashMap<String, Node>) {
    for node in nodemap.values_mut() {
        if let JobStatus::Failed = node.status {
            set_retry_if_possible(node);
        }
    }
}

/// Update the status based on the backend output.
pub fn update_status(uid: &str, nodemap: &mut HashMap<String, Node>, backend: &SlurmBackend) {
    backend.update_status(nodemap);
    schedule_retries(nodemap);

    let mut updated_statuses: HashMap<String, JobStatus> = HashMap::new();
    recoursively_update_status(uid, nodemap, &mut updated_statuses);
    for (k, v) in updated_statuses.into_iter() {
        let updated_node = nodemap.get_mut(&k).unwrap();
        updated_node.status = v;
    }
}

/// Recursively update the status of children.
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

    for child_uid in &node.children {
        recoursively_update_status(child_uid, nodemap, updated_statuses);
    }

    Some(())
}

/// Used to identify the correct status of the parents.
pub struct StatusSelector {
    parent_statuses: Vec<JobStatus>,
}
impl StatusSelector {
    /// Canonical way of creating the selector.
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

    /// Get the correct parent statuses for the child.
    ///
    /// Required because nodes like If provide a different status
    /// for different childs.
    pub fn get_parent_statuses(
        uid: &str,
        nodemap: &HashMap<String, Node>,
    ) -> HashMap<String, JobStatus> {
        let node = nodemap.get(uid).unwrap();
        node.parents
            .iter()
            .map(|p| (&p.parent_type, nodemap.get(&p.uid).unwrap()))
            .map(|(ptype, n)| (n.uid.to_string(), n.get_status_for_child(ptype)))
            .collect()
    }

    /// Decide which protocol should be followed to get the status from parents.
    fn get_updated_status(&self, node_behavior: &NodeBehavior) -> JobStatus {
        match node_behavior {
            NodeBehavior::OneOfNode { .. } => self.get_oneof_status(),
            _ => self.get_default_status(),
        }
    }

    /// Define the new status for most node types.
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

    /// OneOf is special as failed or skipped parents do not cause
    /// it to fail or be skipped.
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

    /// Verify that all parent have completed successfully.
    fn all_parents_completed(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Completed { .. }))
    }

    /// Check if at least one parent completed successfully.
    fn some_parents_completed(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Completed { .. }))
    }

    /// Check if any parent failed.
    fn some_parents_failed(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Failed))
    }

    /// Check if all parents have been skipped.
    fn all_parents_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Skipped))
    }

    /// Check if at least one parent has been skipped.
    fn some_parents_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .any(|x| matches!(x, JobStatus::Skipped))
    }

    /// Check if every parent has failed or has been skipped.
    fn all_parents_failed_or_skipped(&self) -> bool {
        self.parent_statuses
            .iter()
            .all(|x| matches!(x, JobStatus::Failed) | matches!(x, JobStatus::Skipped))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::model::{NodeResult, Parent, ParentType};

    /// Verify the correct OneOf status update.
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

    /// Verify the default status update.
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

    /// Find the correct updated status for OneOf nodes.
    #[test]
    fn get_oneof_updated_status() {
        let selector = StatusSelector {
            parent_statuses: vec![JobStatus::Failed, JobStatus::Completed(NodeResult::Node)],
        };
        let node_behavior = NodeBehavior::OneOfNode;
        matches!(
            selector.get_updated_status(&node_behavior),
            JobStatus::ReadyForSubmission
        );
    }

    /// Check the correct updated status for the default protocol.
    #[test]
    fn get_default_updated_status() {
        let selector = StatusSelector {
            parent_statuses: vec![
                JobStatus::Failed,
                JobStatus::Completed(NodeResult::If(true)),
            ],
        };
        let node_behavior = NodeBehavior::RootNode;
        matches!(
            selector.get_updated_status(&node_behavior),
            JobStatus::Failed
        );
    }

    /// Get a complete nodemap for testing purposes.
    fn get_test_nodemap() -> HashMap<String, Node> {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p1"),
            Node {
                uid: String::from("p1"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
                children: Vec::new(),
            },
        );
        nodemap.insert(
            String::from("p2"),
            Node {
                uid: String::from("p2"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
                children: Vec::new(),
            },
        );
        nodemap.insert(
            String::from("c"),
            Node {
                uid: String::from("c"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: vec![Parent {
                    parent_type: ParentType::Output {
                        key: String::from("p2"),
                    },
                    uid: String::from("p2"),
                }],
                children: Vec::new(),
            },
        );
        nodemap
    }

    /// Test the correct parent status retrieval.
    #[test]
    fn get_parent_statuses() {
        let nodemap = get_test_nodemap();
        let parent_statuses = StatusSelector::get_parent_statuses("c", &nodemap);
        assert_eq!(
            parent_statuses,
            HashMap::from([("p2".to_string(), JobStatus::NotSubmitted)])
        );
    }

    /// Check the selector parent statuses.
    #[test]
    fn get_selector_creation() {
        let nodemap = get_test_nodemap();
        let updated_statuses = HashMap::from([("p2".to_string(), JobStatus::Failed)]);
        let selector = StatusSelector::new("c", &nodemap, &updated_statuses);
        assert_eq!(selector.parent_statuses, vec![JobStatus::Failed]);
    }

    /// Test the status recursive update.
    #[test]
    fn check_recoursive_update() {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p"),
            Node {
                uid: String::from("p"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::Completed(NodeResult::Node),
                parents: Vec::new(),
                children: vec![String::from("c")],
            },
        );

        nodemap.insert(
            String::from("c"),
            Node {
                uid: String::from("c"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: vec![Parent {
                    parent_type: ParentType::Output {
                        key: String::from("p"),
                    },
                    uid: String::from("p"),
                }],
                children: Vec::new(),
            },
        );

        let mut updated_statuses = HashMap::new();
        recoursively_update_status("p", &nodemap, &mut updated_statuses);
        let child_status = updated_statuses.get("c").unwrap();
        assert!(matches!(child_status, JobStatus::ReadyForSubmission))
    }

    /// End-to-end status update test.
    #[test]
    fn e2e() {
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("p"),
            Node {
                uid: String::from("p"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::Completed(NodeResult::Node),
                parents: Vec::new(),
                children: vec![String::from("c")],
            },
        );

        nodemap.insert(
            String::from("c"),
            Node {
                uid: String::from("c"),
                output_artifacts: Vec::new(),
                output_used: false,
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: vec![Parent {
                    parent_type: ParentType::Output {
                        key: String::from("p"),
                    },
                    uid: String::from("p"),
                }],
                children: Vec::new(),
            },
        );

        let backend = SlurmBackend;
        update_status("p", &mut nodemap, &backend);

        let child = nodemap.get("c").unwrap();
        assert!(matches!(child.status, JobStatus::ReadyForSubmission))
    }
}
