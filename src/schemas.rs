use crate::nodes::Node;
use crate::settings::Cfg;
use crate::status::Status;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::Cow, collections::HashMap, fmt, path::PathBuf};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DAGMeta {
    pub pipeline_name: String,
    pub hash: String,
    pub timestamp: String,
    pub extra: Value,
    pub import_path: String,
    pub kwargs: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskMeta {
    /// Task function name
    pub fn_name: String,
    /// Task name. By default it's equal to the function name
    pub name: String,
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

// Task execution mode
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ExecMode {
    /// Wrap an existing task
    #[serde(rename = "wrap")]
    Wrap,
    /// External script
    #[serde(rename = "ext")]
    Ext,
}

/// Task static input kwargs.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Kwarg {
    /// Input key in the function signature.
    pub key: String,
    /// Static input value.
    pub value: Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Cmd {
    /// Slurm
    #[serde(rename = "sbatch")]
    Sbatch,
    /// Local execution
    #[serde(rename = "bash")]
    Bash,
}
impl fmt::Display for Cmd {
    /// For getting the command argument
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cmd = match self {
            Self::Sbatch => "sbatch",
            Self::Bash => "bash",
        };
        write!(f, "{cmd}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Scope {
    /// Local task
    #[serde(rename = "local")]
    Local,
    /// Global task
    #[serde(rename = "global")]
    Global,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ScriptContent {
    pub content: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ScriptPath {
    pub path: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Script {
    Script(ScriptContent),
    ScriptPath(ScriptPath),
}

/// Artifacts.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Artifact {
    /// Artifact name.
    pub name: String,
    /// Artifact path.
    pub path: PathBuf,
}

/// Parent types.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParentKind {
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Parent {
    /// Parent UID.
    pub uid: usize,
    /// Relashionship.
    pub kind: ParentKind,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DAG {
    pub meta: DAGMeta,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Checkpoint<'a> {
    #[serde(borrow)]
    pub cfg: Cow<'a, Cfg>,
    #[serde(borrow)]
    pub meta: Cow<'a, DAGMeta>,
    #[serde(borrow)]
    pub nodes: Cow<'a, [Node]>,
    #[serde(borrow)]
    pub statuses: Cow<'a, [Status]>,
    #[serde(borrow)]
    pub try_nums: Cow<'a, [usize]>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct SlurmOverride {
    pub job_name: Option<String>,
    pub nodes: Option<usize>,
    pub partition: Option<String>,
    pub qos: Option<String>,
    pub gpus_per_node: Option<usize>,
    pub ntasks_per_node: Option<String>,
    pub output: Option<String>,
    pub error: Option<String>,
    pub account: Option<String>,
    pub cpus_per_task: Option<usize>,
    pub mem: Option<String>,
    pub time: Option<String>,
}

// TODO use default
impl SlurmOverride {
    pub fn new() -> Self {
        SlurmOverride {
            job_name: None,
            nodes: None,
            partition: None,
            qos: None,
            gpus_per_node: None,
            ntasks_per_node: None,
            output: None,
            error: None,
            account: None,
            cpus_per_task: None,
            mem: None,
            time: None,
        }
    }
}
