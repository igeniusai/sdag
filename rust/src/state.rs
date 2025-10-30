use crate::model::DAG;
use std::fs;
use std::io;
use std::path::PathBuf;

pub trait StateManager {
    fn prepare(&self, dag: &DAG) -> io::Result<()>;
    fn copy_dag_into_working_dir(&self, path: &PathBuf) -> io::Result<u64>;
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

    fn delete_dir_if_exist(&self) -> io::Result<()> {
        return fs::remove_dir_all(&self.pipeline_dir);
    }

    fn create_working_dir(&self, dag: &DAG) -> io::Result<()> {
        fs::create_dir_all(&self.pipeline_dir)?;
        for node in &dag.nodes {
            let path = self.pipeline_dir.join(&node.uid);
            fs::create_dir(path)?;
        }
        Ok(())
    }
}

impl StateManager for LocalDirState {
    fn prepare(&self, dag: &DAG) -> io::Result<()> {
        let res = self.delete_dir_if_exist();
        if let Err(_) = res {
            println!("Failed to delete pipeline folder, it didn't exist")
        }
        self.create_working_dir(dag)
    }

    fn copy_dag_into_working_dir(&self, path: &PathBuf) -> io::Result<u64> {
        let dst_path = self.pipeline_dir.join(&self.pipeline_fname);
        fs::copy(path, dst_path)
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
        let res = manager.delete_dir_if_exist();
        assert!(matches!(res, Ok(())));
        assert!(!manager.pipeline_dir.is_dir());
    }

    #[test]
    fn missing_pipeline_dir_is_fine() {
        let pipeline_dir = get_tmp_dir().join("pipe2");
        let manager = LocalDirState::new(pipeline_dir);
        let res = manager.delete_dir_if_exist();
        assert!(matches!(res, Err(_)));
        assert!(!manager.pipeline_dir.is_dir());
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
        let res = manager.prepare(&dag);
        assert!(matches!(res, Ok(_)));
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

    #[test]
    fn get_pipeline_dir() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let manager = LocalDirState::new(pipeline_dir.clone());
        assert_eq!(*manager.get_pipeline_dir(), pipeline_dir);
    }

    #[test]
    fn copy_dag() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let src_dir = get_tmp_dir();
        let src = src_dir.join("src_pipeline.json");
        fs::create_dir_all(&pipeline_dir).unwrap();
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(&src, r#"{"name": "pipeline"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir.clone());
        let res = manager.copy_dag_into_working_dir(&src);
        assert!(matches!(res, Ok(_)));
        assert!(pipeline_dir.join("pipeline.json").exists());
    }
}
