use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum JobStatus {
    NotSubmitted,
    ReadyForSubmission,
    Running(String),
    Completed,
    Skipped,
    Failed,
}

impl JobStatus {
    fn initial() -> Self {
        JobStatus::NotSubmitted
    }
}

fn default_init_branch() -> Option<bool> {
    None
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Parent {
    pub name: String,
    pub uid: String,
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
        launch_script: String,
        return_type: Option<String>,
        children: Vec<String>,
    },
    IfNode {
        #[serde(default = "default_init_branch")]
        selected: Option<bool>,
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
            selected,
            true_branch,
            false_branch,
        } = &self.behavior
            && let Some(cond) = selected
        {
            let branch = if *cond { true_branch } else { false_branch };
            let is_selected = branch.iter().any(|x| x == uid);
            if is_selected {
                JobStatus::Completed
            } else {
                JobStatus::Skipped
            }
        } else {
            self.status.clone()
        }
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
