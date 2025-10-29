use crate::model::DAG;
use std::fs;
use std::io;
use std::path::PathBuf;

pub trait StateManager {
    fn prepare(&self, dag: &DAG);
    fn get_pipeline_dir(&self) -> &PathBuf;
    fn copy_output(&self, src_uid: &str, dst_uid: &str) -> io::Result<u64>;
    fn read_output(&self, uid: &str) -> io::Result<String>;
}

#[derive(Debug, Clone)]
pub struct LocalDirState {
    pipeline_dir: PathBuf,
    pipeline_fname: String,
    output_fname: String,
}

impl LocalDirState {
    pub fn new(pipeline_dir: PathBuf) -> Self {
        Self {
            pipeline_dir,
            pipeline_fname: String::from("pipeline.json"),
            output_fname: String::from("output.json"),
        }
    }

    fn delete_dir_if_exist(&self) {
        let res = fs::remove_dir_all(&self.pipeline_dir);
        if let Err(_) = res {
            println!("Failed to remove pipeline folder, it likely didn't exist");
        }
    }

    fn create_dir_and_save_dag(&self, dag: &DAG) {
        let res = fs::create_dir_all(&self.pipeline_dir);
        if let Err(_) = res {
            panic!("Pipeline home directory creation failed");
        }

        let pipeline_json = serde_json::to_string(&dag).unwrap();
        let path = self.pipeline_dir.join(&self.pipeline_fname);
        let res = fs::write(path, pipeline_json);
        if let Err(_) = res {
            panic!("Failed to save the pipeline json file");
        }
    }

    fn create_node_subdirs(&self, dag: &DAG) {
        for node in &dag.nodes {
            let path = self.pipeline_dir.join(&node.uid);
            let res = fs::create_dir(path);
            if let Err(_) = res {
                panic!("Failed to create stage subfolder");
            }
        }
    }
}

impl StateManager for LocalDirState {
    fn prepare(&self, dag: &DAG) {
        self.delete_dir_if_exist();
        self.create_dir_and_save_dag(dag);
        self.create_node_subdirs(dag);
    }

    fn copy_output(&self, src_uid: &str, dst_uid: &str) -> io::Result<u64> {
        let src_path = self.pipeline_dir.join(src_uid).join(&self.output_fname);
        let dst_path = self.pipeline_dir.join(dst_uid).join(&self.output_fname);
        fs::copy(src_path, dst_path)
    }

    fn read_output(&self, uid: &str) -> io::Result<String> {
        let path = self.pipeline_dir.join(uid).join(&self.output_fname);
        fs::read_to_string(path)
    }

    fn get_pipeline_dir(&self) -> &PathBuf {
        &self.pipeline_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{JobStatus, Node, NodeBehavior};
    use std::env;
    use uuid::Uuid;

    fn get_tmp_dir() -> PathBuf {
        env::temp_dir().join(Uuid::new_v4().to_string())
    }

    #[test]
    fn delete_old_folder() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let nested = pipeline_dir.join("0");
        fs::create_dir_all(&nested).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        manager.delete_dir_if_exist();
        assert!(!manager.pipeline_dir.is_dir());
    }

    #[test]
    fn missing_pipeline_dir_is_fine() {
        let pipeline_dir = get_tmp_dir().join("pipe2");
        let manager = LocalDirState::new(pipeline_dir);
        manager.delete_dir_if_exist();
        assert!(!manager.pipeline_dir.is_dir());
    }

    #[test]
    fn create_dir_and_save_dag() {
        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir);

        let dag = DAG {
            name: String::from("pipeline-name"),
            creation_dt: String::from("2025-01-01 09:10:10"),
            nodes: Vec::new(),
        };

        manager.create_dir_and_save_dag(&dag);
        assert!(manager.pipeline_dir.is_dir());
        assert!(manager.pipeline_dir.join("pipeline.json").is_file());
    }

    #[test]
    fn create_entire_structure() {
        let dag = DAG {
            name: String::from("pipeline-name"),
            creation_dt: String::from("2025-01-01 09:10:10"),
            nodes: vec![Node {
                uid: String::from("0"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir);
        manager.prepare(&dag);

        assert!(manager.pipeline_dir.join("pipeline.json").is_file());
        assert!(manager.pipeline_dir.join("0").is_dir());
    }

    #[test]
    fn read_output() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        let content = r#"{"output": true}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("output.json"), content).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        let output = manager.read_output("0").unwrap();
        assert_eq!(output, content);
    }

    #[test]
    fn copy_output_from_zero_to_one() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let src_path = pipeline_dir.join("0");
        let dst_path = pipeline_dir.join("1");
        let content = r#"{"output": true}"#;
        fs::create_dir_all(&src_path).unwrap();
        fs::write(src_path.join("output.json"), content).unwrap();
        fs::create_dir(&dst_path).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        manager.copy_output("0", "1").unwrap();
        assert!(dst_path.join("output.json").is_file());
    }
}
