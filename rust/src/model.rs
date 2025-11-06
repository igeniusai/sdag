use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Artifact {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskOutput {
    pub artifacts: Vec<Artifact>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum NodeResult {
    Node,
    Task(String),
    If(bool),
    OneOf(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum JobStatus {
    NotSubmitted,
    ReadyForSubmission,
    Running(String),
    Completed(NodeResult),
    Skipped,
    Failed,
}
impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let repr = match self {
            Self::NotSubmitted => String::from("Not Submitted"),
            Self::ReadyForSubmission => String::from("Ready for Submission"),
            Self::Running(job_id) => format!("Running ({job_id})"),
            Self::Completed(_) => String::from("Completed"),
            Self::Skipped => String::from("Skipped"),
            Self::Failed => String::from("Failed"),
        };
        write!(f, "{repr}")
    }
}
impl JobStatus {
    fn initial() -> Self {
        JobStatus::NotSubmitted
    }
}

fn initial_try_num() -> u32 {
    0
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum ParentType {
    Logical,
    Artifact { key: String, name: String },
    Output { key: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Parent {
    pub uid: String,
    pub parent_type: ParentType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum NodeBehavior {
    RootNode {
        children: Vec<String>,
    },
    EndNode {
        children: Vec<String>,
    },
    TaskNode {
        fname: String,
        caching: bool,
        #[serde(default = "initial_try_num")]
        try_num: u32,
        retries: u32,
        launch_script: String,
        children: Vec<String>,
    },
    IfNode {
        true_branch: Vec<String>,
        false_branch: Vec<String>,
    },
    OneOfNode {
        children: Vec<String>,
    },
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Node {
    pub uid: String,
    pub behavior: NodeBehavior,
    #[serde(default = "JobStatus::initial")]
    pub status: JobStatus,
    #[serde(default = "Vec::new")]
    pub parents: Vec<Parent>,
}

impl Node {
    pub fn get_status_for_child(&self, uid: &str) -> JobStatus {
        if let NodeBehavior::IfNode {
            true_branch,
            false_branch,
        } = &self.behavior
            && let JobStatus::Completed(NodeResult::If(cond)) = self.status
        {
            let branch = if cond { true_branch } else { false_branch };
            let is_selected = branch.iter().any(|x| x == uid);
            if !is_selected {
                return JobStatus::Skipped;
            }
        }
        self.status.clone()
    }

    pub fn get_all_children(&self) -> Vec<String> {
        match &self.behavior {
            NodeBehavior::IfNode {
                true_branch,
                false_branch,
                ..
            } => true_branch
                .iter()
                .chain(false_branch.iter())
                .cloned()
                .collect(),
            NodeBehavior::OneOfNode { children }
            | NodeBehavior::RootNode { children }
            | NodeBehavior::EndNode { children }
            | NodeBehavior::TaskNode { children, .. } => children.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DAG {
    pub name: String,
    pub creation_dt: String,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BooleanOutput {
    pub output: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn get_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode {
                children: vec![String::from("c")],
            },
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
        };

        assert!(matches!(
            n.get_status_for_child("c"),
            JobStatus::Completed(NodeResult::Node)
        ))
    }

    #[test]
    fn get_if_true_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode {
                true_branch: vec![String::from("c1")],
                false_branch: vec![String::from("c2")],
            },
            status: JobStatus::Completed(NodeResult::If(true)),
            parents: Vec::new(),
        };

        assert!(matches!(
            n.get_status_for_child("c1"),
            JobStatus::Completed(NodeResult::If(true))
        ))
    }

    #[test]
    fn get_if_false_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode {
                true_branch: vec![String::from("c1")],
                false_branch: vec![String::from("c2")],
            },
            status: JobStatus::Completed(NodeResult::If(true)),
            parents: Vec::new(),
        };

        assert!(matches!(n.get_status_for_child("c2"), JobStatus::Skipped))
    }

    #[test]
    fn get_if_failed_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode {
                true_branch: vec![String::from("c1")],
                false_branch: vec![String::from("c2")],
            },
            status: JobStatus::Failed,
            parents: Vec::new(),
        };

        assert!(matches!(n.get_status_for_child("c1"), JobStatus::Failed))
    }

    #[test]
    fn if_get_all_children() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode {
                true_branch: vec![String::from("c1")],
                false_branch: vec![String::from("c2")],
            },
            status: JobStatus::Failed,
            parents: Vec::new(),
        };

        let mut children = n.get_all_children();
        children.sort();
        assert_eq!(children, vec![String::from("c1"), String::from("c2")])
    }

    #[test]
    fn node_get_all_children() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::RootNode {
                children: vec![String::from("c1"), String::from("c2")],
            },
            status: JobStatus::Failed,
            parents: Vec::new(),
        };

        let mut children = n.get_all_children();
        children.sort();
        assert_eq!(children, vec![String::from("c1"), String::from("c2")])
    }
}
