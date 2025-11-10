use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, path::PathBuf};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskMeta {
    pub fname: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Artifact {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskOutput {
    pub output: Value,
    #[serde(default = "Vec::new")]
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
    Branch { branch: bool },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Parent {
    pub uid: String,
    pub parent_type: ParentType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InputKwarg {
    pub key: String,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum NodeBehavior {
    RootNode,
    EndNode,
    IfNode,
    OneOfNode,
    TaskNode {
        fname: String,
        caching: bool,
        #[serde(default = "initial_try_num")]
        try_num: u32,
        retries: u32,
        launch_script: String,
        #[serde(default = "Vec::new")]
        input_kwargs: Vec<InputKwarg>,
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
    #[serde(default = "Vec::new")]
    pub children: Vec<String>,
}
impl Node {
    pub fn get_status_for_child(&self, ptype: &ParentType) -> JobStatus {
        if let NodeBehavior::IfNode = self.behavior
            && let ParentType::Branch { branch: child_b } = ptype
            && let JobStatus::Completed(NodeResult::If(branch)) = self.status
            && branch != *child_b
        {
            JobStatus::Skipped
        } else {
            self.status.clone()
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
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Completed(NodeResult::Node),
            parents: Vec::new(),
            children: vec![String::from("c")],
        };

        let ptype = ParentType::Logical;

        assert!(matches!(
            n.get_status_for_child(&ptype),
            JobStatus::Completed(NodeResult::Node)
        ))
    }

    #[test]
    fn get_if_true_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode,
            status: JobStatus::Completed(NodeResult::If(true)),
            parents: Vec::new(),
            children: vec![String::from("c1"), String::from("c2")],
        };
        let ptype = ParentType::Branch { branch: true };
        assert!(matches!(
            n.get_status_for_child(&ptype),
            JobStatus::Completed(NodeResult::If(true))
        ))
    }

    #[test]
    fn get_if_false_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode,
            status: JobStatus::Completed(NodeResult::If(true)),
            parents: Vec::new(),
            children: vec![String::from("c1"), String::from("c2")],
        };
        let ptype = ParentType::Branch { branch: false };
        assert!(matches!(n.get_status_for_child(&ptype), JobStatus::Skipped))
    }

    #[test]
    fn get_if_failed_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            behavior: NodeBehavior::IfNode,
            status: JobStatus::Failed,
            parents: Vec::new(),
            children: vec![String::from("c1"), String::from("c2")],
        };
        let ptype = ParentType::Branch { branch: false };
        assert!(matches!(n.get_status_for_child(&ptype), JobStatus::Failed))
    }
}
