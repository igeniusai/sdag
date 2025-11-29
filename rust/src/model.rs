//! Models to parse DAGs.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::Borrow, fmt, path::PathBuf};

/// Task metadata as written in the working dir.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskMeta {
    /// Name of the task function. Used for caching.
    pub fname: String,
}

/// Artifacts.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Artifact {
    /// Artifact name.
    pub name: String,
    /// Artifact path.
    pub path: PathBuf,
}

/// Task output.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskOutput {
    /// Serialize output value.
    pub output: Value,
    /// Output artifacts.
    #[serde(default = "Vec::new")]
    pub artifacts: Vec<Artifact>,
}

/// Possible successful statuses.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum NodeResult {
    /// Node completed, no special info carried.
    Node,
    /// Completed task. It carries the successful jobid.
    Task(String),
    /// Result of a branch. It carries the selected branch.
    If(bool),
    /// OneOf result. It contains the uid of the selected node.
    OneOf(String),
}

/// Node statuses:
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum JobStatus {
    /// Node not submitted yet.
    NotSubmitted,
    /// Node ready for submission.
    ReadyForSubmission,
    /// Task is running (contains the Slurm jobid).
    Running(String),
    /// Node completed.
    Completed(NodeResult),
    /// Node skipped (e.g., because of a branch).
    Skipped,
    /// Node execution failed.
    Failed,
}

/// Used to display the status in the log table
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
    /// All statuses start as not submitted.
    fn initial() -> Self {
        JobStatus::NotSubmitted
    }
}

/// Set the inial number of tries to zero.
fn initial_try_num() -> u32 {
    0
}

/// Parent types.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum ParentType {
    /// No data exchange, the dependence is only logical.
    Logical,
    /// Node takes a parent artifact as input.
    Artifact {
        /// Key in the task function signature.
        key: String,
        /// Artifact name.
        name: String,
        /// Artifact path
        path: PathBuf,
    },
    /// Node takes the parent output as input.
    Output { key: String },
    /// Branch dependence.
    Branch { branch: bool },
}

/// Node parent.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Parent {
    /// Parent UID.
    pub uid: String,
    /// Relashionship.
    pub parent_type: ParentType,
}

/// Task static input kwargs.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InputKwarg {
    /// Input key in the function signature.
    pub key: String,
    /// Static input value.
    pub value: Value,
}

// Node types.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum NodeBehavior {
    /// Pipeline root. There is only one root per pipeline.
    /// Nested pipelines will also have their root.
    RootNode,
    /// Pipeline end. There is only one root per pipeline.
    /// Nested pipelines will also have their root.
    EndNode,
    /// Branch node. There is only one end per pipeline.
    /// Nested pipelines will also have their end.
    IfNode,
    /// OneOf node.
    OneOfNode,
    /// User-defined tasks.
    TaskNode {
        /// Task function name.
        fname: String,
        /// Caching.
        caching: bool,
        /// Try number.
        #[serde(default = "initial_try_num")]
        try_num: u32,
        /// Number of retries.
        retries: u32,
        /// Slurm sbatch script.
        launch_script: String,
        #[serde(default = "Vec::new")]
        /// Static input kwargs.
        input_kwargs: Vec<InputKwarg>,
    },
}

/// Graph node.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Node {
    // Node unique id.
    pub uid: String,
    // Output used by other nodes.
    pub output_used: bool,
    // Output artifacts.
    pub output_artifacts: Vec<Artifact>,
    // Node type.
    pub behavior: NodeBehavior,
    /// Node status.
    #[serde(default = "JobStatus::initial")]
    pub status: JobStatus,
    /// Node parents.
    #[serde(default = "Vec::new")]
    pub parents: Vec<Parent>,
    /// Children uids.
    #[serde(default = "Vec::new")]
    pub children: Vec<String>,
}
impl Node {
    /// Get the status for a children.
    ///
    /// A children requests the status of their parents to determine
    /// what to do. This status is usually just the parent status with
    /// the notable exception of the IfNode, because a successful status
    /// might cause the child to be skipped.
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
pub struct DAGMetadata {
    /// Pipeline name.
    pub name: String,
    /// Pipeline creation datetime.
    pub creation_dt: String,
}

/// Pipeline JSON file.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DAG<T: Borrow<Node>> {
    /// Pipeline metadata.
    pub meta: DAGMetadata,
    /// Pipeline nodes.
    pub nodes: Vec<T>,
}

/// Output of a condition task.
///
/// Tasks used as conditions must return a Boolean. They are
/// used to define the output of a branch.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BooleanOutput {
    /// Task output.
    pub output: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Not an IfNode, The status is the same of the parent.
    #[test]
    fn get_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            output_artifacts: Vec::new(),
            output_used: false,
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

    /// Child in the selected branch, the status is successful.
    #[test]
    fn get_if_true_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            output_artifacts: Vec::new(),
            output_used: false,
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

    /// Child in the skipped branch, the status is skipped.
    #[test]
    fn get_if_false_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            output_artifacts: Vec::new(),
            output_used: false,
            behavior: NodeBehavior::IfNode,
            status: JobStatus::Completed(NodeResult::If(true)),
            parents: Vec::new(),
            children: vec![String::from("c1"), String::from("c2")],
        };
        let ptype = ParentType::Branch { branch: false };
        assert!(matches!(n.get_status_for_child(&ptype), JobStatus::Skipped))
    }

    /// Parent failed, the status is failed.
    #[test]
    fn get_if_failed_status_for_child() {
        let n = Node {
            uid: String::from("p"),
            output_artifacts: Vec::new(),
            output_used: false,
            behavior: NodeBehavior::IfNode,
            status: JobStatus::Failed,
            parents: Vec::new(),
            children: vec![String::from("c1"), String::from("c2")],
        };
        let ptype = ParentType::Branch { branch: false };
        assert!(matches!(n.get_status_for_child(&ptype), JobStatus::Failed))
    }

    #[test]
    fn serialize_borrowed_dag() {
        let nodemap = HashMap::from([(
            String::from("0"),
            Node {
                uid: String::from("0"),
                output_artifacts: Vec::new(),
                output_used: false,
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::Running(String::from("1234")),
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("fname"),
                    caching: false,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                },
            },
        )]);

        let node = nodemap.get(&String::from("0")).unwrap();
        let dag = DAG {
            meta: DAGMetadata {
                name: String::from("dag"),
                creation_dt: String::from("2011-01-01T09:20:20"),
            },
            nodes: vec![node],
        };

        let serialized = serde_json::to_string(&dag).unwrap();
        let deserialized_dag: DAG<Node> = serde_json::from_str(&serialized).unwrap();
        assert!(matches!(
            deserialized_dag.nodes[0].status,
            JobStatus::Running(_)
        ));
    }
}
