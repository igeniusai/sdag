//! Caching
//!
//! Caching is fairly complicated. The decision about
//! whether a task should be considered as cached depends
//! on several factors:
//! - If the output is not used, only artifacts are checked.
//! - If the output is used, uid, name, input, and artifats must
//!     be compatible for caching to occur.

use crate::model::{
    InputKwarg, JobStatus, Node, NodeBehavior, NodeResult, Parent, ParentType, TaskOutput,
};
use crate::state::StateManager;
use log;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::error::Error;
use std::io;

pub fn replace_cache<T: StateManager>(node: &Node, state: &T) {
    log::debug!("Checking task '{}' cache", node.uid);
    if let JobStatus::Completed(NodeResult::Task(_)) = node.status
        && let NodeBehavior::TaskNode { caching, fname, .. } = &node.behavior
        && *caching
    {
        log::info!("Saving task '{fname}' cache");
        state
            .cache_task(fname, &node.uid)
            .map_err(|e| log::error!("Failed to save cache for task '{fname}': {e}"))
            .ok();
    }
}

/// Read the input data
pub fn read_input_and_cache_tasks<T: StateManager>(nodemap: &mut HashMap<String, Node>, state: &T) {
    let caching_res: HashMap<String, Result<bool, Box<dyn Error>>> = nodemap
        .iter()
        .filter(|(_, node)| matches!(node.status, JobStatus::ReadyForSubmission))
        .filter_map(|(key, node)| get_task_caching_status(key, node, state))
        .collect();

    for (key, result) in caching_res.into_iter() {
        let node = nodemap.get_mut(&key).unwrap();
        set_caching_and_status(node, result);
    }
}

/// Filter out non-task nodes and find the caching result
fn get_task_caching_status<T: StateManager>(
    key: &str,
    node: &Node,
    state: &T,
) -> Option<(String, Result<bool, Box<dyn Error>>)> {
    match &node.behavior {
        NodeBehavior::TaskNode {
            caching,
            fname,
            input_kwargs,
            ..
        } => Some((
            key.to_string(),
            check_task_caching(node, caching, fname, input_kwargs, state),
        )),
        _ => None,
    }
}

/// Set status after the caching algorithm.
/// Any error will cause the stage to fail.
fn set_caching_and_status(node: &mut Node, result: Result<bool, Box<dyn Error>>) {
    match result {
        Err(_) => {
            log::error!("Failed to check input for task '{}'", node.uid);
            node.status = JobStatus::Failed;
        }
        Ok(cached) => {
            if cached {
                log::info!("Task '{}' is cached", node.uid);
                node.status = JobStatus::Completed(NodeResult::Node);
            }
        }
    }
}

/// Check task caching.
/// Here is where artifacts and input data are checked.
fn check_task_caching<T: StateManager>(
    node: &Node,
    caching: &bool,
    fname: &str,
    input_kwargs: &Vec<InputKwarg>,
    state: &T,
) -> Result<bool, Box<dyn Error>> {
    let input = read_input_data(node, input_kwargs, state)?;
    if !*caching | state.copy_cache(fname, &node.uid).is_err() {
        return treat_as_uncached(&node.uid, &input, state);
    }

    match read_cached_input_data(&node.uid, state) {
        Err(_) => {
            return treat_as_uncached(&node.uid, &input, state);
        }
        Ok(cached) => {
            if cached != input {
                return treat_as_uncached(&node.uid, &input, state);
            }
        }
    }

    for artifact in &node.output_artifacts {
        if !artifact.path.exists() {
            return treat_as_uncached(&node.uid, &input, state);
        }
    }
    Ok(true)
}

/// Uncache task
/// Input data for the executions are saved
fn treat_as_uncached<T: StateManager>(
    uid: &str,
    input: &HashMap<String, Value>,
    state: &T,
) -> Result<bool, Box<dyn Error>> {
    save_input(uid, input, state)?;
    Ok(false)
}

/// Read static and parent input data
fn read_input_data<T: StateManager>(
    node: &Node,
    input_kwargs: &Vec<InputKwarg>,
    state: &T,
) -> Result<HashMap<String, Value>, Box<dyn Error>> {
    let mut input_data: HashMap<String, Value> = HashMap::new();
    add_static_input(&mut input_data, input_kwargs);
    for parent in &node.parents {
        add_dynamic_input(&mut input_data, parent, state)?;
    }
    Ok(input_data)
}

/// Add the static input to the input data
fn add_static_input(input_data: &mut HashMap<String, Value>, input_kwargs: &Vec<InputKwarg>) {
    for input_kwarg in input_kwargs {
        input_data.insert(input_kwarg.key.clone(), input_kwarg.value.clone());
    }
}

/// Add the dynamic input to the input data
fn add_dynamic_input<T: StateManager>(
    input_data: &mut HashMap<String, Value>,
    parent: &Parent,
    state: &T,
) -> Result<(), Box<dyn Error>> {
    match &parent.parent_type {
        ParentType::Logical | ParentType::Branch { .. } => {}
        ParentType::Artifact { key, path, .. } => {
            let path_str = path.clone().into_os_string().into_string().unwrap();
            input_data.insert(key.to_string(), Value::String(path_str));
        }
        ParentType::Output { key } => {
            let output = state.read_output(&parent.uid)?;
            let parent_output: TaskOutput = serde_json::from_str(&output)?;
            input_data.insert(key.to_string(), parent_output.output);
        }
    }

    Ok(())
}

/// Read input data of the previous run
fn read_cached_input_data<T: StateManager>(
    uid: &str,
    state: &T,
) -> Result<HashMap<String, Value>, Box<dyn Error>> {
    let raw_input = state.read_cached_input(uid)?;
    let input: HashMap<String, Value> = serde_json::from_str(&raw_input)?;
    Ok(input)
}

/// Save the input data
fn save_input<T: StateManager>(
    uid: &str,
    input_data: &HashMap<String, Value>,
    state: &T,
) -> Result<(), io::Error> {
    let input_str = serde_json::to_string(input_data)?;
    state.save_input(uid, &input_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Artifact, DAG, InputKwarg, JobStatus, Node, NodeBehavior, Parent, ParentType,
    };
    use crate::state::{LocalDirState, tests::get_tmp_dir};
    use std::{env, fs, path::PathBuf};

    /// Mocked state for testing purposes
    struct MockState {
        fake_path: PathBuf,
    }
    impl MockState {
        fn new() -> Self {
            Self {
                fake_path: PathBuf::from("path"),
            }
        }
    }

    /// Mocked state manager
    impl StateManager for MockState {
        fn prepare(&self, _dag: &DAG<Node>) -> io::Result<()> {
            Ok(())
        }
        fn copy_output(&self, _src_uid: &str, _dst_uid: &str) -> std::io::Result<u64> {
            std::io::Result::Ok(1)
        }
        fn read_output(&self, _uid: &str) -> std::io::Result<String> {
            std::io::Result::Ok(String::from(
                r#"{"output":true, "artifacts": [{"name": "a", "path": "/"}]}"#,
            ))
        }
        fn get_pipeline_dir(&self) -> &PathBuf {
            &self.fake_path
        }
        fn copy_dag_into_working_dir(&self, _path: &PathBuf) -> io::Result<u64> {
            Ok(1)
        }
        fn save_input(&self, _uid: &str, _input: &str) -> io::Result<()> {
            Ok(())
        }
        fn read_cached_input(&self, _uid: &str) -> io::Result<String> {
            Ok(String::from(r#"{"a": true}"#))
        }

        fn read_checkpoint(&self) -> io::Result<String> {
            Ok(String::from("checkpoint"))
        }

        fn save_checkpoint(&self, _dag: &DAG<&Node>) -> io::Result<()> {
            Ok(())
        }

        fn validate_checkpoint(&self, _dag: &DAG<Node>) -> Result<(), String> {
            Ok(())
        }
        fn cache_task(&self, _fname: &str, _uid: &str) -> io::Result<u64> {
            Ok(1)
        }
        fn copy_cache(&self, _fname: &str, _uid: &str) -> io::Result<u64> {
            Ok(1)
        }
    }

    #[test]
    fn test_replace_cache() {
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::Completed(NodeResult::Task("1234".to_string())),
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: Vec::new(),
            },
        };

        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        let state = LocalDirState::new(&home_dir, pipeline_name);
        let uid = "1";
        let res_path = home_dir.join(pipeline_name).join(uid);
        let cache_path = home_dir.join(".cache").join("fname");

        fs::create_dir_all(&res_path).unwrap();
        fs::create_dir_all(&cache_path).unwrap();
        fs::write(res_path.join("input.json"), "hello").unwrap();
        fs::write(res_path.join("output.json"), "hello").unwrap();
        fs::write(res_path.join("meta.json"), "hello").unwrap();

        replace_cache(&node, &state);
        assert!(cache_path.join("input.json").exists());
        assert!(cache_path.join("output.json").exists());
        assert!(cache_path.join("meta.json").exists());
    }

    /// Test the parent output retrieval.
    #[test]
    fn test_add_dynamic_input() {
        let state = MockState::new();
        let mut input_data: HashMap<String, Value> = HashMap::new();
        let parent = Parent {
            uid: "0".to_string(),
            parent_type: ParentType::Output {
                key: "key".to_string(),
            },
        };
        add_dynamic_input(&mut input_data, &parent, &state).unwrap();
        assert_eq!(
            input_data,
            HashMap::from([("key".to_string(), Value::Bool(true))])
        );
    }

    /// Test the static output retrieval
    #[test]
    fn test_add_static_input() {
        let mut input_data: HashMap<String, Value> = HashMap::new();
        let input_kwargs = vec![
            InputKwarg {
                key: "a".to_string(),
                value: Value::String("test".to_string()),
            },
            InputKwarg {
                key: "b".to_string(),
                value: Value::Bool(true),
            },
        ];

        add_static_input(&mut input_data, &input_kwargs);
        assert_eq!(
            input_data,
            HashMap::from([
                ("a".to_string(), Value::String("test".to_string())),
                ("b".to_string(), Value::Bool(true)),
            ])
        );
    }

    /// Test the input save
    #[test]
    fn test_save_input() {
        let input_data = HashMap::from([("b".to_string(), Value::Bool(true))]);
        let state = MockState::new();
        let uid = "0".to_string();
        save_input(&uid, &input_data, &state).unwrap();
    }

    /// Check the cached input read
    #[test]
    fn test_read_cached_input() {
        let state = MockState::new();
        let uid = "0".to_string();
        let cached_input = read_cached_input_data(&uid, &state).unwrap();
        assert_eq!(
            cached_input,
            HashMap::from([("a".to_string(), Value::Bool(true))])
        );
    }

    /// Check the complete input reading
    #[test]
    fn test_read_input_data() {
        let state = MockState::new();
        let input_kwargs = vec![InputKwarg {
            key: "a".to_string(),
            value: Value::Bool(true),
        }];
        let node = Node {
            uid: String::from("1"),
            output_used: true,
            output_artifacts: Vec::new(),
            parents: vec![Parent {
                uid: "0".to_string(),
                parent_type: ParentType::Output {
                    key: "b".to_string(),
                },
            }],
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: input_kwargs.clone(),
            },
        };

        let input_data = read_input_data(&node, &input_kwargs, &state).unwrap();
        assert_eq!(
            input_data,
            HashMap::from([
                ("a".to_string(), Value::Bool(true)),
                ("b".to_string(), Value::Bool(true)),
            ])
        );
    }

    /// No output, no artifacts, task is cached.
    #[test]
    fn test_task_caching_no_output_no_artifacts() {
        let state = MockState::new();
        let input_kwargs: Vec<InputKwarg> = vec![InputKwarg {
            key: "a".to_string(),
            value: Value::Bool(true),
        }];
        let caching = true;
        let fname = String::from("fname");
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: fname.clone(),
                caching: caching,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: input_kwargs.clone(),
            },
        };

        assert!(check_task_caching(&node, &caching, &fname, &input_kwargs, &state).unwrap());
    }

    /// Task not marked as cachable.
    #[test]
    fn test_task_no_caching_no_output_no_artifacts() {
        let state = MockState::new();
        let input_kwargs: Vec<InputKwarg> = vec![InputKwarg {
            key: "a".to_string(),
            value: Value::Bool(true),
        }];
        let caching = false;
        let fname = String::from("fname");
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: fname.clone(),
                caching: caching,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: input_kwargs.clone(),
            },
        };

        assert!(!check_task_caching(&node, &caching, &fname, &input_kwargs, &state).unwrap());
    }

    /// Artifact exists, task is cached
    #[test]
    fn test_task_caching_existing_artifact() {
        let path = env::temp_dir().join("tmp");
        fs::create_dir_all(&path).unwrap();

        let state = MockState::new();
        let input_kwargs: Vec<InputKwarg> = vec![InputKwarg {
            key: "a".to_string(),
            value: Value::Bool(true),
        }];
        let caching = true;
        let fname = String::from("fname");
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: vec![Artifact {
                name: "existing".to_string(),
                path: path.clone(),
            }],
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: fname.clone(),
                caching: caching,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: input_kwargs.clone(),
            },
        };

        assert!(check_task_caching(&node, &caching, &fname, &input_kwargs, &state).unwrap());
    }

    /// Artifact does not exist, output is not cached
    #[test]
    fn test_task_caching_missing_artifact() {
        let state = MockState::new();
        let input_kwargs: Vec<InputKwarg> = vec![InputKwarg {
            key: "a".to_string(),
            value: Value::Bool(true),
        }];
        let caching = true;
        let fname = String::from("fname");
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: vec![Artifact {
                name: "missing".to_string(),
                path: PathBuf::from("/a/missing/path"),
            }],
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: fname.clone(),
                caching: caching,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: input_kwargs.clone(),
            },
        };

        assert!(!check_task_caching(&node, &caching, &fname, &input_kwargs, &state).unwrap());
    }

    /// Full caching test
    #[test]
    fn test_e2e() {
        let node = Node {
            uid: String::from("1"),
            output_used: false,
            output_artifacts: Vec::new(),
            parents: Vec::new(),
            children: Vec::new(),
            status: JobStatus::ReadyForSubmission,
            behavior: NodeBehavior::TaskNode {
                fname: String::from("fname"),
                caching: true,
                try_num: 0,
                retries: 0,
                launch_script: String::from("submit.sh"),
                input_kwargs: vec![InputKwarg {
                    key: "a".to_string(),
                    value: Value::Bool(true),
                }],
            },
        };

        let mut nodemap = HashMap::from([("0".to_string(), node)]);
        let state = MockState::new();

        read_input_and_cache_tasks(&mut nodemap, &state);
        let node = nodemap.get("0").unwrap();
        assert!(matches!(node.status, JobStatus::Completed(_)))
    }
}
