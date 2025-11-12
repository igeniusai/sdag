//! State management.
//!
//! Every interaction with the file system is segregated here.

use crate::model::{Node, NodeBehavior, TaskMeta, DAG};
use log;
use serde_json;
use std::fs;
use std::io;
use std::path::PathBuf;

/// State management interface.
pub trait StateManager {
    /// Prepare the working directory.
    fn prepare(&self, dag: &DAG) -> io::Result<()>;
    /// Copy the JSON as-is into the working directory.
    fn copy_dag_into_working_dir(&self, path: &PathBuf) -> io::Result<u64>;
    /// Get the path to the working directory to set the env.
    fn get_pipeline_dir(&self) -> &PathBuf;
    /// Copy the output of a node into another one. Useful for OneOf
    fn copy_output(&self, src_uid: &str, dst_uid: &str) -> io::Result<u64>;
    /// Read the output of a node.
    fn read_output(&self, uid: &str) -> io::Result<String>;
    /// Save the input of a node.
    fn save_input(&self, uid: &str, input: &str) -> Result<(), io::Error>;
    /// Read the cached input of a node.
    fn read_cached_input(&self, uid: &str) -> io::Result<String>;
}

/// Local directory state.
#[derive(Debug, Clone)]
pub struct LocalDirState {
    /// Pipeline directory path.
    pipeline_dir: PathBuf,
    /// DAG JSON filename in the working dir.
    pipeline_fname: String,
    /// Node output filename.
    output_fname: String,
    /// Node input filename.
    input_fname: String,
    /// Metadata filename.
    meta_fname: String,
}

impl LocalDirState {
    /// Standardized way to create a state manager.
    pub fn new(pipeline_dir: PathBuf) -> Self {
        Self {
            // Pipeline folder name in the working directory.
            pipeline_dir,
            // Pipeline filename convention.
            pipeline_fname: String::from("pipeline.json"),
            // Output filename convention.
            output_fname: String::from("output.json"),
            // Input filename convention.
            input_fname: String::from("input.json"),
            // Metadata filename convention.
            meta_fname: String::from("meta.json"),
        }
    }

    /// Create a working directory.
    fn create_working_dir(&self, dag: &DAG) -> io::Result<()> {
        let res = fs::create_dir_all(&self.pipeline_dir);
        if let Err(_) = res {
            log::debug!("pipeline dir creation failed, it likely already exists.")
        }

        for node in &dag.nodes {
            if let NodeBehavior::TaskNode { fname, .. } = &node.behavior {
                let path = self.pipeline_dir.join(&node.uid);
                if !path.is_dir() {
                    fs::create_dir(&path)?;
                    self.write_meta(&path, fname)?
                }
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

    /// Clear unnecessary cache at the beginning of the pipeline.
    fn clear_cache(&self, dag: &DAG) -> Result<(), io::Error> {
        for entry in fs::read_dir(&self.pipeline_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }

            if self.dir_must_be_deleted(&path, dag) {
                let _ = fs::remove_dir_all(path)
                    .map_err(|e| log::error!("Failed cache dir deletion: {e}"));
            }
        }
        Ok(())
    }

    /// Check if a caching directory must be deleted.
    ///
    /// Conditions to keep cached directories:
    /// - The node is a task.
    /// - Caching must be enabled.
    /// - The task function name correspods.
    /// - The Node unique id remains the same.
    /// - Artifacts must exist.
    fn dir_must_be_deleted(&self, path: &PathBuf, dag: &DAG) -> bool {
        if let Some(dirname) = path.file_name().and_then(|n| n.to_str()) {
            for node in &dag.nodes {
                if self.is_node_matching(node, path, dirname) {
                    return false;
                }
            }
        }
        true
    }

    /// Read the task name.
    fn read_task_name(&self, path: &PathBuf) -> Result<String, io::Error> {
        let meta_path = path.join(&self.meta_fname);
        let content = fs::read_to_string(meta_path)?;
        let meta: TaskMeta = serde_json::from_str(&content)?;
        Ok(meta.fname)
    }

    /// Check if the caching rules are respected.
    fn is_node_matching(&self, node: &Node, path: &PathBuf, dirname: &str) -> bool {
        if node.uid == dirname
            && let NodeBehavior::TaskNode { fname, caching, .. } = &node.behavior
            && *caching
            && let Ok(task_name) = self.read_task_name(path)
        {
            return task_name == *fname;
        }
        false
    }
}

/// Implement the StateManager for local file systems.
impl StateManager for LocalDirState {
    /// Prepare the working directory in the local fs.
    fn prepare(&self, dag: &DAG) -> io::Result<()> {
        if self.pipeline_dir.is_dir() {
            self.clear_cache(dag)?;
        }
        self.create_working_dir(dag)
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

        let src_input = src_dir.join(&self.input_fname);
        let dst_input = dst_dir.join(&self.input_fname);
        fs::copy(src_input, dst_input)?;

        let src_output = src_dir.join(&self.output_fname);
        let dst_output = dst_dir.join(&self.output_fname);
        fs::copy(src_output, dst_output)
    }

    /// Read the output of a node.
    fn read_output(&self, uid: &str) -> io::Result<String> {
        let path = self.pipeline_dir.join(uid).join(&self.output_fname);
        fs::read_to_string(path)
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
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::model::{JobStatus, Node, NodeBehavior};
    use std::env;
    use uuid::Uuid;

    pub fn get_tmp_dir() -> PathBuf {
        env::temp_dir().join(Uuid::new_v4().to_string())
    }

    /// The working directory is correctly created.
    #[test]
    fn test_working_dir_creation() {
        let dag = DAG {
            name: String::from("dag"),
            creation_dt: String::from("1900-01-01T09:20:20"),
            nodes: vec![Node {
                uid: String::from("0"),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("fname"),
                    caching: false,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                },
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir.clone());
        let res = manager.create_working_dir(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(pipeline_dir.join("0").join("meta.json").is_file());
    }

    /// The metadata is correctly written.
    #[test]
    fn write_metadata() {
        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let meta_dir = pipeline_dir.clone();
        fs::create_dir_all(&pipeline_dir).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        manager.write_meta(&meta_dir, "foo").unwrap();

        let meta_path = meta_dir.join("meta.json");
        let meta_str = fs::read_to_string(meta_path).unwrap();
        let meta: TaskMeta = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta.fname, "foo");
    }

    /// Deletion is handle even when the working dir is missing.
    #[test]
    fn clear_cache_no_dirs() {
        let dag = DAG {
            name: String::from("dag"),
            creation_dt: String::from("1900-01-01T09:20:20"),
            nodes: Vec::new(),
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        fs::create_dir_all(&pipeline_dir).unwrap();
        let manager = LocalDirState::new(pipeline_dir);
        assert!(matches!(manager.clear_cache(&dag), Ok(_)));
    }

    /// Caching is not enabled
    #[test]
    fn dir_must_be_deleted() {
        let dag = DAG {
            name: String::from("dag"),
            creation_dt: String::from("1900-01-01T09:20:20"),
            nodes: vec![Node {
                uid: String::from("0"),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("fname"),
                    caching: false,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                },
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(manager.dir_must_be_deleted(&path, &dag));
    }

    /// Everything corresponds, the directory is kept.
    #[test]
    fn dir_must_not_be_deleted() {
        let dag = DAG {
            name: String::from("dag"),
            creation_dt: String::from("1900-01-01T09:20:20"),
            nodes: vec![Node {
                uid: String::from("0"),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("fname"),
                    caching: true,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                },
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(!manager.dir_must_be_deleted(&path, &dag));
    }

    /// The caching rules are verified.
    #[test]
    fn node_is_matching() {
        let node = Node {
            uid: String::from("0"),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::NotSubmitted,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            },
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(manager.is_node_matching(&node, &path, "0"));
    }

    /// No caching, the uid changed.
    #[test]
    fn node_not_matching_because_of_id() {
        let node = Node {
            uid: String::from("1"),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::NotSubmitted,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            },
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(!manager.is_node_matching(&node, &path, "0"));
    }

    /// No caching, the node is not a task.
    #[test]
    fn node_not_matching_because_of_behavior() {
        let node = Node {
            uid: String::from("0"),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::NotSubmitted,
            behavior: NodeBehavior::RootNode {},
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(!manager.is_node_matching(&node, &path, "0"));
    }

    /// No caching, caching is not enabled.
    #[test]
    fn node_not_matching_because_of_caching() {
        let node = Node {
            uid: String::from("0"),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::NotSubmitted,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: false,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            },
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(!manager.is_node_matching(&node, &path, "0"));
    }

    /// No caching, the function name is different.
    #[test]
    fn node_not_matching_because_of_task_name() {
        let node = Node {
            uid: String::from("0"),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::NotSubmitted,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("different_name"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("lauch.sh"),
                input_kwargs: Vec::new(),
            },
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let path = pipeline_dir.join("0");
        let meta_path = path.join("meta.json");
        fs::create_dir_all(&path).unwrap();
        fs::write(&meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        assert!(!manager.is_node_matching(&node, &path, "0"));
    }

    /// The folder must not be create when caching does not happen.
    #[test]
    fn do_not_create_folder() {
        let dag = DAG {
            name: String::from("pipeline-name"),
            creation_dt: String::from("2025-01-01 09:10:10"),
            nodes: vec![Node {
                uid: String::from("0"),
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
                children: Vec::new(),
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir);
        let res = manager.prepare(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(!manager.pipeline_dir.join("0").is_dir());
    }

    /// Read the node output.
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

    /// Copy the output of a node into another one.
    #[test]
    fn copy_output_from_zero_to_one() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let src_path = pipeline_dir.join("0");
        let dst_path = pipeline_dir.join("1");
        let content = r#"{"output": true}"#;
        fs::create_dir_all(&src_path).unwrap();
        fs::write(src_path.join("output.json"), content).unwrap();
        fs::write(src_path.join("input.json"), content).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        manager.copy_output("0", "1").unwrap();
        assert!(dst_path.join("output.json").is_file());
    }

    /// Check the pipeline directory path retrieval.
    #[test]
    fn get_pipeline_dir() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let manager = LocalDirState::new(pipeline_dir.clone());
        assert_eq!(*manager.get_pipeline_dir(), pipeline_dir);
    }

    /// Copy the JSON into the working directory.
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

    /// Create the complete working dir structure when caching occurs.
    #[test]
    fn create_entire_structure_with_caching() {
        let dag = DAG {
            name: String::from("pipeline-name"),
            creation_dt: String::from("2025-01-01 09:10:10"),
            nodes: vec![
                Node {
                    uid: String::from("0"),
                    behavior: NodeBehavior::RootNode,
                    status: JobStatus::NotSubmitted,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
                Node {
                    uid: String::from("1"),
                    behavior: NodeBehavior::TaskNode {
                        fname: String::from("fname"),
                        caching: true,
                        retries: 0,
                        try_num: 0,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    },
                    status: JobStatus::NotSubmitted,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("1");
        fs::create_dir_all(&path).unwrap();

        let content = r#"{"output": true}"#;
        let output_path = path.join("output.json");
        let input_path = path.join("input.json");
        let meta_path = path.join("meta.json");

        fs::write(output_path, content).unwrap();
        fs::write(input_path, r#"{}"#).unwrap();
        fs::write(meta_path, r#"{"fname": "fname"}"#).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        let res = manager.prepare(&dag);

        assert!(matches!(res, Ok(_)));
        assert!(manager.pipeline_dir.join("1").join("output.json").is_file());
        assert!(manager.pipeline_dir.join("1").join("input.json").is_file());
        assert!(manager.pipeline_dir.join("1").join("meta.json").is_file());
    }

    /// If the working directory is missing, it must be created without errors.
    #[test]
    fn prepare_without_pipeline_dir() {
        let dag = DAG {
            name: String::from("dag"),
            creation_dt: String::from("1900-01-01T09:20:20"),
            nodes: vec![Node {
                uid: String::from("0"),
                parents: Vec::new(),
                children: Vec::new(),
                status: JobStatus::NotSubmitted,
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("fname"),
                    caching: false,
                    try_num: 0,
                    retries: 0,
                    launch_script: String::from("lauch.sh"),
                    input_kwargs: Vec::new(),
                },
            }],
        };

        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir.clone());
        let res = manager.prepare(&dag);
        assert!(matches!(res, Ok(_)));
        assert!(pipeline_dir.join("0").join("meta.json").exists())
    }

    /// Cached input does not exist.
    #[test]
    #[should_panic]
    fn dont_read_cached_input() {
        let pipeline_dir = get_tmp_dir().join("pipeline-name");
        let manager = LocalDirState::new(pipeline_dir);
        manager.read_cached_input("1").unwrap();
    }

    /// Cached input is correctly read.
    #[test]
    fn read_cached_input() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("1");
        let content = r#"{"a": "input-value"}"#;
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("input.json"), content).unwrap();

        let manager = LocalDirState::new(pipeline_dir);
        let input = manager.read_cached_input("1").unwrap();
        assert_eq!(input, content)
    }

    /// Check the input data is correctly saved.
    #[test]
    fn test_input_save() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("1");
        fs::create_dir_all(&path).unwrap();

        let input = r#"{"a": "input-value"}"#;
        let manager = LocalDirState::new(pipeline_dir);
        manager.save_input("1", input).unwrap();

        assert!(path.join(manager.input_fname).exists());
    }
}
