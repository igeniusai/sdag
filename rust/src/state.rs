//! State management.
//!
//! Every interaction with the file system is segregated here.

use crate::model::NodeResult;
use crate::model::{Artifact, DAG, JobStatus, Node, NodeBehavior, TaskMeta, TaskOutput};
use log;
use serde_json;
use std::fs;
use std::io;
use std::path::PathBuf;

/// State management interface.
pub trait StateManager {
    /// Prepare the working directory.
    fn prepare(&self, dag: &DAG<Node>) -> io::Result<()>;
    /// Copy the JSON as-is into the working directory.
    fn copy_dag_into_working_dir(&self, path: &PathBuf) -> io::Result<u64>;
    /// Get the path to the working directory to set the env.
    fn get_pipeline_dir(&self) -> &PathBuf;
    /// Copy the output of a node into another one. Useful for OneOf
    fn copy_output(&self, src_uid: &str, dst_uid: &str) -> io::Result<u64>;
    /// Read the output of a node.
    fn read_output(&self, uid: &str) -> io::Result<String>;
    /// Save the input of a node.
    fn save_empty_output(&self, uid: &str, artifacts: &Vec<Artifact>) -> io::Result<()>;
    /// Save the input of a node.
    fn save_input(&self, uid: &str, input: &str) -> Result<(), io::Error>;
    /// Read the cached input of a node.
    fn read_cached_input(&self, uid: &str) -> io::Result<String>;
    /// Read last checkpoint
    fn read_checkpoint(&self) -> io::Result<String>;
    /// Validate the checkpoint.
    fn validate_checkpoint(&self, dag: &DAG<Node>) -> Result<(), String>;
    /// Save checkpoint.
    fn save_checkpoint(&self, dag: &DAG<&Node>) -> io::Result<()>;
    /// Cache a task
    fn cache_task(&self, fname: &str, uid: &str) -> io::Result<u64>;
    /// Copy cache back to the input
    fn copy_cache(&self, fname: &str, uid: &str) -> io::Result<u64>;
}

/// Local directory state.
#[derive(Debug, Clone)]
pub struct LocalDirState {
    /// Working directory path.
    home_dir: PathBuf,
    /// Pipeline directory path.
    pipeline_dir: PathBuf,
    /// Caching directory path.
    cache_dir: PathBuf,
    /// DAG JSON filename in the working dir.
    pipeline_fname: String,
    /// Node output filename.
    output_fname: String,
    /// Node input filename.
    input_fname: String,
    /// Metadata filename.
    meta_fname: String,
    /// Checkpoint filename
    checkpoint_fname: String,
}

impl LocalDirState {
    /// Standardized way to create a state manager.
    pub fn new(home_dir: &PathBuf, pipeline_name: &str) -> Self {
        Self {
            // Home directory
            home_dir: home_dir.clone(),
            // Pipeline folder name in the working directory.
            pipeline_dir: home_dir.join(&pipeline_name),
            // Caching dir
            cache_dir: home_dir.join(".cache"),
            // Pipeline filename convention.
            pipeline_fname: String::from("pipeline.json"),
            // Output filename convention.
            output_fname: String::from("output.json"),
            // Input filename convention.
            input_fname: String::from("input.json"),
            // Metadata filename convention.
            meta_fname: String::from("meta.json"),
            // Checkpoint filename convention.
            checkpoint_fname: String::from("checkpoint.json"),
        }
    }

    fn create_home_dir_if_missing(&self) {
        let res = fs::create_dir_all(&self.home_dir);
        if let Err(_) = res {
            log::debug!("Home dir creation failed, it likely already exists.")
        }

        let res = fs::create_dir(&self.cache_dir);
        if let Err(_) = res {
            log::debug!("Caching dir creation failed, it likely already exists.")
        }
    }

    /// Create a working directory.
    fn create_pipeline_dir(&self, dag: &DAG<Node>) -> io::Result<()> {
        fs::remove_dir_all(&self.pipeline_dir)
            .map_err(|e| log::debug!("Failed to remove pipeline dir: {e}"))
            .ok();
        fs::create_dir(&self.pipeline_dir)?;

        for node in &dag.nodes {
            if let NodeBehavior::TaskNode(task) = &node.behavior {
                let path = self.pipeline_dir.join(&node.uid);
                fs::create_dir(&path)?;
                self.write_meta(&path, &task.fname)?;
            }
        }
        Ok(())
    }

    /// Write metadata.
    fn write_meta(&self, path: &PathBuf, fname: &str) -> Result<(), io::Error> {
        let meta = TaskMeta {
            fname: String::from(fname),
        };
        let content = serde_json::to_string(&meta)?;
        let dst = path.join(&self.meta_fname);
        fs::write(dst, &content)
    }

    fn validate_node_from_checkpoint(&self, node: &Node) -> Result<(), String> {
        let working_dir = self.pipeline_dir.join(&node.uid);
        if !working_dir.is_dir() {
            let err = format!("uid '{}' directory not found", node.uid);
            return Err(err);
        }

        if let JobStatus::Completed(NodeResult::Task(_)) = node.status {
            self.validate_completed_task_for_checkpoint(node, &working_dir)?;
        }

        Ok(())
    }

    fn validate_completed_task_for_checkpoint(
        &self,
        node: &Node,
        working_dir: &PathBuf,
    ) -> Result<(), String> {
        let path_out = working_dir.join(&self.output_fname);
        if !path_out.is_file() {
            let err = format!("uid '{}' output not found", node.uid);
            return Err(err);
        }

        let path_meta = working_dir.join(&self.meta_fname);
        if !path_meta.is_file() {
            let err = format!("uid '{}' metadata not found", node.uid);
            return Err(err);
        }

        Ok(())
    }

    fn copy_task_data(&self, src_path: &PathBuf, dst_path: &PathBuf) -> io::Result<u64> {
        let output_from = src_path.join("output.json");
        let output_to = dst_path.join("output.json");
        fs::copy(&output_from, output_to)?;

        let input_from = src_path.join("input.json");
        let input_to = dst_path.join("input.json");
        fs::copy(&input_from, input_to)?;

        let meta_from = src_path.join("meta.json");
        let meta_to = dst_path.join("meta.json");
        fs::copy(&meta_from, meta_to)
    }
}

/// Implement the StateManager for local file systems.
impl StateManager for LocalDirState {
    /// Prepare the working directory in the local fs.
    fn prepare(&self, dag: &DAG<Node>) -> io::Result<()> {
        self.create_home_dir_if_missing();
        self.create_pipeline_dir(dag)
    }

    /// Copy the DAG JSON as-is into the working directory.
    fn copy_dag_into_working_dir(&self, path: &PathBuf) -> io::Result<u64> {
        let dst_path = self.pipeline_dir.join(&self.pipeline_fname);
        fs::copy(path, dst_path)
    }

    /// Copy the node content into the node working directory.
    fn copy_output(&self, src_uid: &str, dst_uid: &str) -> io::Result<u64> {
        let src_dir = self.pipeline_dir.join(src_uid);
        let dst_dir = self.pipeline_dir.join(dst_uid);
        fs::create_dir(&dst_dir)?;
        self.copy_task_data(&src_dir, &dst_dir)
    }

    /// Read the output of a node.
    fn read_output(&self, uid: &str) -> io::Result<String> {
        let path = self.pipeline_dir.join(uid).join(&self.output_fname);
        fs::read_to_string(path)
    }

    /// Save the empty output of all external
    fn save_empty_output(&self, uid: &str, artifacts: &Vec<Artifact>) -> io::Result<()> {
        let path = self.pipeline_dir.join(uid).join(&self.output_fname);
        let output = TaskOutput {
            output: serde_json::Value::Null,
            artifacts: artifacts.clone(),
        };
        let contents = serde_json::to_string(&output)?;
        fs::write(path, contents)
    }

    /// Get the path to the pipeline folder.
    fn get_pipeline_dir(&self) -> &PathBuf {
        &self.pipeline_dir
    }

    /// Read the input of a cached node.
    fn read_cached_input(&self, uid: &str) -> io::Result<String> {
        let path = self.pipeline_dir.join(uid).join(&self.input_fname);
        fs::read_to_string(path)
    }

    /// Save the input in the working directory.
    fn save_input(&self, uid: &str, input: &str) -> Result<(), io::Error> {
        let path = self.pipeline_dir.join(uid).join(&self.input_fname);
        fs::write(path, input)
    }

    /// Read a checkpoint
    fn read_checkpoint(&self) -> io::Result<String> {
        let path = self.pipeline_dir.join(&self.checkpoint_fname);
        fs::read_to_string(path)
    }

    /// Read a checkpoint
    fn save_checkpoint(&self, dag: &DAG<&Node>) -> io::Result<()> {
        let path = self.pipeline_dir.join(&self.checkpoint_fname);
        let checkpoint = serde_json::to_string(dag)?;
        fs::write(path, checkpoint)
    }

    /// Validate checkpoint
    fn validate_checkpoint(&self, dag: &DAG<Node>) -> Result<(), String> {
        for node in &dag.nodes {
            if let NodeBehavior::TaskNode { .. } = node.behavior {
                self.validate_node_from_checkpoint(&node)?;
            }
        }
        Ok(())
    }

    fn cache_task(&self, fname: &str, uid: &str) -> io::Result<u64> {
        let src = self.pipeline_dir.join(uid);
        let dst = self.cache_dir.join(fname);

        fs::remove_dir_all(&dst).ok();
        fs::create_dir(&dst)?;
        self.copy_task_data(&src, &dst)
    }

    fn copy_cache(&self, fname: &str, uid: &str) -> io::Result<u64> {
        let cache = self.cache_dir.join(fname);
        let dst = self.pipeline_dir.join(uid);
        self.copy_task_data(&cache, &dst)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::model::{Cmd, DAGMetadata, ExecMode, JobStatus, Node, NodeBehavior, Task};
    use std::env;
    use uuid::Uuid;

    pub fn get_tmp_dir() -> PathBuf {
        env::temp_dir().join(Uuid::new_v4().to_string())
    }

    /// The working directory is correctly created.
    #[test]
    fn test_working_dir_creation() {
        let dag = DAG {
            meta: DAGMetadata {
                name: String::from("dag"),
                creation_dt: String::from("1900-01-01T09:20:20"),
            },
            nodes: vec![Node {
                uid: String::from("0"),
                output_artifacts: Vec::new(),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode(Task {
                    fname: String::from("fname"),
                    caching: false,
                    mode: ExecMode::Wrap,
                    cmd: Cmd::Sbatch,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                }),
            }],
        };

        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        fs::create_dir_all(home_dir.join(pipeline_name)).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        let res = manager.create_pipeline_dir(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(
            home_dir
                .join(pipeline_name)
                .join("0")
                .join("meta.json")
                .is_file()
        );
    }

    /// The metadata is correctly written.
    #[test]
    fn write_metadata() {
        let pipeline_name = "pipeline";
        let pipeline_dir = get_tmp_dir().join(pipeline_name);
        let meta_dir = pipeline_dir.clone();
        fs::create_dir_all(&pipeline_dir).unwrap();

        let home_dir = get_tmp_dir();
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.write_meta(&meta_dir, "foo").unwrap();

        let meta_path = meta_dir.join("meta.json");
        let meta_str = fs::read_to_string(meta_path).unwrap();
        let meta: TaskMeta = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta.fname, "foo");
    }

    /// The folder must not be create when caching does not happen.
    #[test]
    fn do_not_create_folder() {
        let dag = DAG {
            meta: DAGMetadata {
                name: String::from("pipeline-name"),
                creation_dt: String::from("2025-01-01 09:10:10"),
            },
            nodes: vec![Node {
                uid: String::from("0"),
                output_artifacts: Vec::new(),
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
                children: Vec::new(),
            }],
        };

        let pipeline_name = "pipeline";
        let home_dir = get_tmp_dir();
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        let res = manager.prepare(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(!manager.pipeline_dir.join("0").is_dir());
    }

    /// Read the node output.
    #[test]
    fn read_output() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let stage_path = home_dir.join(pipeline_name).join("0");
        let content = r#"{"output": true}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("output.json"), content).unwrap();

        let manager = LocalDirState::new(&home_dir, "pipeline");
        let output = manager.read_output("0").unwrap();
        assert_eq!(output, content);
    }

    /// Copy the output of a node into another one.
    #[test]
    fn copy_output_from_zero_to_one() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let src_path = home_dir.join(pipeline_name).join("0");
        let dst_path = home_dir.join(pipeline_name).join("1");
        let content = r#"{"output": true}"#;

        fs::create_dir_all(&src_path).unwrap();
        fs::write(src_path.join("output.json"), content).unwrap();
        fs::write(src_path.join("input.json"), content).unwrap();
        fs::write(src_path.join("meta.json"), content).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.copy_output("0", "1").unwrap();

        assert!(dst_path.join("output.json").is_file());
        assert!(dst_path.join("input.json").is_file());
        assert!(dst_path.join("meta.json").is_file());
    }

    #[test]
    fn test_cache_task_no_dir() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let fname = "fname";
        let src_path = home_dir.join(pipeline_name).join("0");
        let content = r#"{"output": true}"#;
        let cache_path = home_dir.join(".cache");

        fs::create_dir_all(&src_path).unwrap();
        fs::create_dir_all(&cache_path).unwrap();
        fs::write(src_path.join("output.json"), content).unwrap();
        fs::write(src_path.join("input.json"), content).unwrap();
        fs::write(src_path.join("meta.json"), content).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.cache_task(fname, "0").unwrap();

        assert!(cache_path.join(fname).join("output.json").is_file());
        assert!(cache_path.join(fname).join("input.json").is_file());
        assert!(cache_path.join(fname).join("meta.json").is_file());
    }

    #[test]
    fn test_replace_cache() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let fname = "fname";
        let src_path = home_dir.join(pipeline_name).join("0");
        let cache_path = home_dir.join(".cache").join(fname);
        let content = r#"{"output": true}"#;

        fs::create_dir_all(&src_path).unwrap();
        fs::create_dir_all(&cache_path).unwrap();
        fs::write(cache_path.join("meta.json"), "hello world").unwrap();
        fs::write(src_path.join("output.json"), content).unwrap();
        fs::write(src_path.join("input.json"), content).unwrap();
        fs::write(src_path.join("meta.json"), content).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.cache_task(fname, "0").unwrap();

        assert!(cache_path.join("output.json").is_file());
        assert!(cache_path.join("input.json").is_file());

        let meta = fs::read_to_string(cache_path.join("meta.json")).unwrap();
        assert_eq!(meta, content);
    }

    /// Check the pipeline directory path retrieval.
    #[test]
    fn get_pipeline_dir() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let pipeline_dir = home_dir.join(pipeline_name);
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        assert_eq!(*manager.get_pipeline_dir(), pipeline_dir);
    }

    /// Copy the JSON into the working directory.
    #[test]
    fn copy_dag() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let pipeline_dir = home_dir.join(pipeline_name);
        let src_dir = get_tmp_dir();
        let src = src_dir.join("src_pipeline.json");

        fs::create_dir_all(&src_dir).unwrap();
        fs::create_dir_all(&pipeline_dir).unwrap();
        fs::write(&src, r#"{"name": "pipeline"}"#).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        let res = manager.copy_dag_into_working_dir(&src);

        assert!(matches!(res, Ok(_)));
        assert!(pipeline_dir.join("pipeline.json").exists());
    }

    /// Create the complete working dir structure when caching occurs.
    #[test]
    fn create_entire_structure() {
        let pipeline_name = "pipeline";
        let fname = "fname";

        let dag = DAG {
            meta: DAGMetadata {
                name: String::from(pipeline_name),
                creation_dt: String::from("2025-01-01 09:10:10"),
            },
            nodes: vec![
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::RootNode,
                    status: JobStatus::NotSubmitted,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from(fname),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        retries: 0,
                        try_num: 0,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::NotSubmitted,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ],
        };

        let home_dir = get_tmp_dir();
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.prepare(&dag).unwrap();
        assert!(manager.pipeline_dir.join("1").join("meta.json").is_file());
    }

    /// If the working directory is missing, it must be created without errors.
    #[test]
    fn prepare_without_pipeline_dir() {
        let dag = DAG {
            meta: DAGMetadata {
                name: String::from("dag"),
                creation_dt: String::from("1900-01-01T09:20:20"),
            },
            nodes: vec![Node {
                uid: String::from("0"),
                output_artifacts: Vec::new(),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode(Task {
                    fname: String::from("fname"),
                    caching: false,
                    mode: ExecMode::Wrap,
                    cmd: Cmd::Sbatch,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                }),
            }],
        };

        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        let res = manager.prepare(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(
            home_dir
                .join(pipeline_name)
                .join("0")
                .join("meta.json")
                .exists()
        )
    }

    /// Cached input does not exist.
    #[test]
    #[should_panic]
    fn dont_read_cached_input() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.read_cached_input("1").unwrap();
    }

    /// Cached input is correctly read.
    #[test]
    fn read_cached_input() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let path = home_dir.join(pipeline_name).join("1");
        let content = r#"{"a": "input-value"}"#;
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("input.json"), content).unwrap();

        let manager = LocalDirState::new(&home_dir, pipeline_name);
        let input = manager.read_cached_input("1").unwrap();
        assert_eq!(input, content)
    }

    /// Check the input data is correctly saved.
    #[test]
    fn test_input_save() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let path = home_dir.join(pipeline_name).join("1");
        fs::create_dir_all(&path).unwrap();

        let input = r#"{"a": "input-value"}"#;
        let manager = LocalDirState::new(&home_dir, pipeline_name);
        manager.save_input("1", input).unwrap();

        assert!(path.join(manager.input_fname).exists());
    }

    #[test]
    #[should_panic]
    fn node_dir_not_found_in_checkpoint_val() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        fs::create_dir_all(&home_dir.join(pipeline_name)).unwrap();

        let state = LocalDirState::new(&home_dir, pipeline_name);
        let node = Node {
            uid: String::from("0"),
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::Running(String::from("1234")),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("fname"),
                caching: false,
                mode: ExecMode::Wrap,
                cmd: Cmd::Sbatch,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            }),
        };

        state.validate_node_from_checkpoint(&node).unwrap();
    }

    #[test]
    #[should_panic]
    fn output_not_found_in_checkpoint_val() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let node_dir = home_dir.join(pipeline_name).join("0");
        fs::create_dir_all(&node_dir).unwrap();

        let state = LocalDirState::new(&home_dir, pipeline_name);
        let node = Node {
            uid: String::from("0"),
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::Completed(NodeResult::Task(String::from("1234"))),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("fname"),
                caching: false,
                mode: ExecMode::Wrap,
                cmd: Cmd::Sbatch,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            }),
        };

        state.validate_node_from_checkpoint(&node).unwrap();
    }

    #[test]
    #[should_panic]
    fn meta_not_found_in_checkpoint_val() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let node_dir = home_dir.join(pipeline_name).join("0");
        fs::create_dir_all(&node_dir).unwrap();

        let output_path = node_dir.join("output.json");
        fs::write(&output_path, r#"{"output":null}"#).unwrap();

        let state = LocalDirState::new(&home_dir, pipeline_name);
        let node = Node {
            uid: String::from("0"),
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::Completed(NodeResult::Task(String::from("1234"))),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("fname"),
                caching: false,
                mode: ExecMode::Wrap,
                cmd: Cmd::Sbatch,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            }),
        };

        state.validate_node_from_checkpoint(&node).unwrap();
    }

    #[test]
    fn test_save_emtpy_output() {
        let home_dir = get_tmp_dir();
        let uid = "0";
        let pipeline_name = "pipeline";
        let node_dir = home_dir.join(pipeline_name).join(&uid);
        fs::create_dir_all(&node_dir).unwrap();

        let state = LocalDirState::new(&home_dir, pipeline_name);
        let artifacts = vec![Artifact {
            name: "artifact".to_string(),
            path: PathBuf::from("a/path"),
        }];

        state.save_empty_output(&uid, &artifacts).unwrap();
        let s = fs::read_to_string(node_dir.join("output.json")).unwrap();
        let output: TaskOutput = serde_json::from_str(&s).unwrap();
        assert!(matches!(output.output, serde_json::Value::Null));
        assert_eq!(output.artifacts, artifacts);
    }

    #[test]
    fn checkpoint_node_correctly_validated() {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let node_dir = home_dir.join(pipeline_name).join("0");
        fs::create_dir_all(&node_dir).unwrap();

        let output_path = node_dir.join("output.json");
        fs::write(&output_path, r#"{"output":null}"#).unwrap();

        let meta_path = node_dir.join("meta.json");
        fs::write(&meta_path, r#"{"fname":"fname"}"#).unwrap();

        let state = LocalDirState::new(&home_dir, pipeline_name);
        let node = Node {
            uid: String::from("0"),
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::Completed(NodeResult::Task(String::from("1234"))),
            behavior: NodeBehavior::TaskNode(Task {
                fname: String::from("fname"),
                caching: false,
                mode: ExecMode::Wrap,
                cmd: Cmd::Sbatch,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            }),
        };

        state.validate_node_from_checkpoint(&node).unwrap();
    }
}
