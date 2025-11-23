//! Checkpointing
//! Checkpoints can be used to reload

use crate::model::{DAG, DAGMetadata, Node};
use crate::state::StateManager;
use std::collections::HashMap;
use std::error::Error;
use std::io;

#[derive(Clone, Debug)]
pub struct Checkpointer {
    pub meta: DAGMetadata,
    pub restart: bool,
}

impl Checkpointer {
    pub fn load_checkpoint<T: StateManager>(&self, state: &T) -> Result<DAG<Node>, Box<dyn Error>> {
        let checkpoint = state.read_checkpoint()?;
        let dag: DAG<Node> = serde_json::from_str(&checkpoint)?;
        state.validate_checkpoint(&dag)?;

        Ok(dag)
    }

    pub fn save_checkpoint<T: StateManager>(
        &self,
        nodemap: &HashMap<String, Node>,
        state: &T,
    ) -> io::Result<()> {
        let nodes: Vec<&Node> = nodemap.values().collect();
        let dag = DAG {
            meta: self.meta.clone(),
            nodes,
        };
        state.save_checkpoint(&dag)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::model::{DAGMetadata, JobStatus, NodeBehavior};
    use crate::state::LocalDirState;

    use super::*;
    use crate::state::tests::get_tmp_dir;

    #[test]
    fn save_and_load_checkpoint() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let task_dir = pipeline_dir.join("0");
        fs::create_dir_all(&task_dir).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let checkpointer = Checkpointer {
            meta: DAGMetadata {
                name: String::from("pipeline"),
                creation_dt: String::from("2025-01-01 09:20:20"),
            },
            restart: false,
        };

        let nodemap = HashMap::from([(
            String::from("0"),
            Node {
                uid: String::from("0"),
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

        checkpointer.save_checkpoint(&nodemap, &state).unwrap();
        let dag = checkpointer.load_checkpoint(&state).unwrap();

        assert_eq!(dag.meta.name, "pipeline");
        assert!(matches!(dag.nodes[0].status, JobStatus::Running(_)))
    }

    // The checkpoint folder does not exist
    #[test]
    #[should_panic]
    fn fail_checkpoint_save() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let state = LocalDirState::new(pipeline_dir);
        let checkpointer = Checkpointer {
            meta: DAGMetadata {
                name: String::from("pipeline"),
                creation_dt: String::from("2025-01-01 09:20:20"),
            },
            restart: false,
        };

        let nodemap = HashMap::from([(
            String::from("0"),
            Node {
                uid: String::from("0"),
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

        checkpointer.save_checkpoint(&nodemap, &state).unwrap();
    }

    // The checkpoint folder does not exist
    #[test]
    #[should_panic]
    fn fail_checkpoint_read() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let state = LocalDirState::new(pipeline_dir);
        let checkpointer = Checkpointer {
            meta: DAGMetadata {
                name: String::from("pipeline"),
                creation_dt: String::from("2025-01-01 09:20:20"),
            },
            restart: false,
        };
        checkpointer.load_checkpoint(&state).unwrap();
    }
}
