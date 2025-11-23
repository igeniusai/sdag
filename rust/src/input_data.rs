//! Manage the input data.
//!
//! Here is where most of the complex caching logic is implemented.

use crate::model::{InputKwarg, Node, Parent, ParentType, TaskOutput};
use crate::state::StateManager;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::error::Error;
use std::io;

/// Input data manager.
pub struct InputDataHandler<'a, T: StateManager> {
    /// Node unique id.
    pub uid: &'a str,
    /// File system interaction.
    pub state: &'a T,
}

impl<'a, T: StateManager> InputDataHandler<'a, T> {
    /// Build the input data by combining static and dynamic input.
    pub fn read_input_data(
        &self,
        nodemap: &HashMap<String, Node>,
        input_kwargs: &Vec<InputKwarg>,
    ) -> Result<HashMap<String, Value>, Box<dyn Error>> {
        let node = nodemap.get(self.uid).unwrap();
        let mut input_data: HashMap<String, Value> = HashMap::new();
        self.add_static_input(&mut input_data, input_kwargs);
        for parent in &node.parents {
            self.add_dynamic_input(&mut input_data, parent)?;
        }
        Ok(input_data)
    }

    /// Check if a node is cached.
    pub fn is_cached(&self, input_data: &HashMap<String, Value>) -> bool {
        if let Err(_) = self.check_if_output_and_artifacts_exist() {
            return false;
        }

        match self.read_cached_input_data() {
            Err(_) => false,
            Ok(cached_input) => *input_data == cached_input,
        }
    }

    /// Save the input data with the help of the state.
    pub fn save_input(&self, input_data: &HashMap<String, Value>) -> Result<(), io::Error> {
        let input_str = serde_json::to_string(input_data)?;
        self.state.save_input(self.uid, &input_str)
    }

    /// Read the cached input data for comparing it with the actual ones.
    fn read_cached_input_data(&self) -> Result<HashMap<String, Value>, Box<dyn Error>> {
        let raw_input = self.state.read_cached_input(self.uid)?;
        let input: HashMap<String, Value> = serde_json::from_str(&raw_input)?;
        Ok(input)
    }

    /// Verify all artifacts exist for caching.
    fn check_if_output_and_artifacts_exist(&self) -> Result<(), Box<dyn Error>> {
        let output = self.state.read_output(self.uid)?;
        let task_output: TaskOutput = serde_json::from_str(&output)?;
        for artifact in &task_output.artifacts {
            if !artifact.path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Artifact '{:?}' not found", artifact.path),
                )
                .into());
            }
        }
        Ok(())
    }

    /// Add the static input to the input data.
    fn add_static_input(
        &self,
        input_data: &mut HashMap<String, Value>,
        input_kwargs: &Vec<InputKwarg>,
    ) {
        for input_kwarg in input_kwargs {
            input_data.insert(input_kwarg.key.clone(), input_kwarg.value.clone());
        }
    }

    /// Add the dynamic input to the input data.
    fn add_dynamic_input(
        &self,
        input_data: &mut HashMap<String, Value>,
        parent: &Parent,
    ) -> Result<(), Box<dyn Error>> {
        match &parent.parent_type {
            ParentType::Logical | ParentType::Branch { .. } => {}
            ParentType::Output { key } => {
                self.add_parent_output(input_data, &parent.uid, key)?;
            }
            ParentType::Artifact { key, name } => {
                self.add_parent_artifact(input_data, &parent.uid, key, name)?;
            }
        }
        Ok(())
    }

    /// Add the parent output to the dynamic input data.
    fn add_parent_output(
        &self,
        input_data: &mut HashMap<String, Value>,
        uid: &str,
        key: &str,
    ) -> Result<(), Box<dyn Error>> {
        let data = self.get_parent_output(&uid)?;
        input_data.insert(key.to_string(), data.output.clone());
        Ok(())
    }

    /// Add the parent artifacts to the dynamic input data.
    fn add_parent_artifact(
        &self,
        input_data: &mut HashMap<String, Value>,
        uid: &str,
        key: &str,
        name: &str,
    ) -> Result<(), Box<dyn Error>> {
        let data = self.get_parent_output(&uid)?;
        for artifact in data.artifacts {
            if &artifact.name == name {
                let strpath = artifact.path.into_os_string().into_string().map_err(|s| {
                    let msg = format!("Invalid UTF-8 Path: {:?}", s);
                    Box::<dyn Error>::from(msg)
                })?;
                input_data.insert(key.to_string(), Value::String(strpath));
                break;
            }
        }
        Ok(())
    }

    /// Get the parent output
    fn get_parent_output(&self, uid: &str) -> Result<TaskOutput, Box<dyn Error>> {
        let output = self.state.read_output(uid)?;
        let parent_output: TaskOutput = serde_json::from_str(&output)?;
        Ok(parent_output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Artifact, DAG, InputKwarg, JobStatus, Node, NodeBehavior, Parent, ParentType,
    };
    use crate::state::{LocalDirState, tests::get_tmp_dir};
    use std::{fs, path::PathBuf};

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
            Ok(String::from(r#"{}"#))
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
    }

    /// Test the parent output retrieval.
    #[test]
    fn test_parent_output_retrieval() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let output = handler.get_parent_output("0").unwrap();
        assert_eq!(output.output, true);
        assert_eq!(output.artifacts[0].name, "a");
    }

    /// Test the parent artifact addition to the input data.
    #[test]
    fn test_parent_artifact_addition() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let mut input_data: HashMap<String, Value> = HashMap::new();
        handler
            .add_parent_artifact(&mut input_data, "1", "key", "a")
            .unwrap();

        let arr = [(String::from("key"), Value::String(String::from("/")))];
        let expected = HashMap::from(arr);
        assert_eq!(input_data, expected);
    }

    /// Test the parent output addition to the input data.
    #[test]
    fn test_add_parent_output() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let mut input_data: HashMap<String, Value> = HashMap::new();
        handler
            .add_parent_output(&mut input_data, "1", "key")
            .unwrap();

        let arr = [(String::from("key"), Value::Bool(true))];
        let expected = HashMap::from(arr);
        assert_eq!(input_data, expected);
    }

    /// Test the whole dynamic output addition.
    #[test]
    fn dynamic_output_addition() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let parent = Parent {
            uid: String::from("1"),
            parent_type: ParentType::Output {
                key: String::from("key"),
            },
        };
        let mut input_data: HashMap<String, Value> = HashMap::new();
        handler.add_dynamic_input(&mut input_data, &parent).unwrap();
        let arr = [(String::from("key"), Value::Bool(true))];
        let expected = HashMap::from(arr);
        assert_eq!(input_data, expected);
    }

    /// Check that logical parents are skipped.
    #[test]
    fn dynamic_logical_addition() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let parent = Parent {
            uid: String::from("1"),
            parent_type: ParentType::Logical,
        };
        let mut input_data: HashMap<String, Value> = HashMap::new();
        handler.add_dynamic_input(&mut input_data, &parent).unwrap();
        assert_eq!(input_data, HashMap::new());
    }

    /// Check the dynamic artifct addition to the input data.
    #[test]
    fn dynamic_artifact_addition() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let parent = Parent {
            uid: String::from("1"),
            parent_type: ParentType::Artifact {
                key: String::from("key"),
                name: String::from("a"),
            },
        };
        let mut input_data: HashMap<String, Value> = HashMap::new();
        handler.add_dynamic_input(&mut input_data, &parent).unwrap();
        let arr = [(String::from("key"), Value::String(String::from("/")))];
        let expected = HashMap::from(arr);
        assert_eq!(input_data, expected);
    }

    /// Test the static input addition to the input data.
    #[test]
    fn static_kwarg_addition() {
        let state = MockState::new();
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_kwargs = vec![
            InputKwarg {
                key: String::from("a"),
                value: Value::Bool(true),
            },
            InputKwarg {
                key: String::from("b"),
                value: Value::String(String::from("s")),
            },
        ];
        let mut input_data: HashMap<String, Value> = HashMap::new();
        let expected = HashMap::from([
            (String::from("a"), Value::Bool(true)),
            (String::from("b"), Value::String(String::from("s"))),
        ]);

        handler.add_static_input(&mut input_data, &input_kwargs);
        assert_eq!(input_data, expected)
    }

    /// The output folder does not exist.
    #[test]
    #[should_panic]
    fn output_doesnt_exist() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        handler.check_if_output_and_artifacts_exist().unwrap();
    }

    /// The output data exists.
    #[test]
    fn output_exist() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        let content = r#"{"output": true}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("output.json"), content).unwrap();
        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let res = handler.check_if_output_and_artifacts_exist();
        assert!(matches!(res, Ok(_)));
    }

    /// The artifact is missing.
    #[test]
    #[should_panic]
    fn artifact_doesnt_exist() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        let content = r#"{"output": true, "artifacts": [{"name": "a", "path": "/not/exist"]}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("output.json"), content).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        handler.check_if_output_and_artifacts_exist().unwrap();
    }

    /// The artifact exists.
    #[test]
    fn artifact_exist() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        let content = r#"{"output": true, "artifacts": [{"name": "a", "path": "."}]}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("output.json"), content).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let res = handler.check_if_output_and_artifacts_exist();
        assert!(matches!(res, Ok(_)));
    }

    /// Read the input from the cached file.
    #[test]
    fn read_cached_input() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        let content = r#"{"a": true}"#;
        fs::create_dir_all(&stage_path).unwrap();
        fs::write(stage_path.join("input.json"), content).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let res = handler.read_cached_input_data().unwrap();
        let expected = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert_eq!(res, expected);
    }

    /// Save the input data
    #[test]
    fn save_input_data() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let stage_path = pipeline_dir.join("0");
        fs::create_dir_all(&stage_path).unwrap();
        let data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        handler.save_input(&data).unwrap();
        assert!(stage_path.join("input.json").is_file());
    }

    /// Verify the task is correctly cached.
    #[test]
    fn task_is_cached() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("0");
        let input = r#"{"a": true}"#;
        let output = r#"{"output": true, "artifacts":[]}"#;

        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("output.json"), output).unwrap();
        fs::write(path.join("input.json"), input).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert!(handler.is_cached(&input_data));
    }

    /// Artifact does not exist, the task is not cached.
    #[test]
    fn task_is_not_cached_because_of_artifact() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("0");
        let input = r#"{"a": true}"#;
        let task_output = TaskOutput {
            output: Value::Null,
            artifacts: vec![Artifact {
                name: String::from("name"),
                path: pipeline_dir.join("artifact.txt"),
            }],
        };
        let output = serde_json::to_string(&task_output).unwrap();

        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("output.json"), output).unwrap();
        fs::write(path.join("input.json"), input).unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert!(!handler.is_cached(&input_data));
    }

    /// The artifact exist, so the task is cached.
    #[test]
    fn task_is_cached_because_of_artifact() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("0");
        let input = r#"{"a": true}"#;
        let task_output = TaskOutput {
            output: Value::Null,
            artifacts: vec![Artifact {
                name: String::from("name"),
                path: pipeline_dir.join("artifact.txt"),
            }],
        };
        let output = serde_json::to_string(&task_output).unwrap();

        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("output.json"), output).unwrap();
        fs::write(path.join("input.json"), input).unwrap();
        fs::write(pipeline_dir.join("artifact.txt"), "content").unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert!(handler.is_cached(&input_data));
    }

    /// The task folder does not exist, so the task is not cached.
    #[test]
    fn task_does_not_exist() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };
        let input_data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert!(!handler.is_cached(&input_data));
    }

    /// Complete caching test.
    #[test]
    fn test_caching_full() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("0");
        let input = r#"{"a": true}"#;
        let task_output = TaskOutput {
            output: Value::Null,
            artifacts: vec![Artifact {
                name: String::from("name"),
                path: pipeline_dir.join("artifact.txt"),
            }],
        };
        let output = serde_json::to_string(&task_output).unwrap();

        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("output.json"), output).unwrap();
        fs::write(path.join("input.json"), input).unwrap();
        fs::write(pipeline_dir.join("artifact.txt"), "content").unwrap();

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_data = HashMap::from([(String::from("a"), Value::Bool(true))]);
        assert!(handler.is_cached(&input_data));
    }

    /// Verify the input data is correctly read.
    #[test]
    fn test_read_input_data() {
        let pipeline_dir = get_tmp_dir().join("pipeline");
        let path = pipeline_dir.join("0");
        let parent_path = pipeline_dir.join("1");
        let output = r#"{"output": true}"#;
        fs::create_dir_all(&path).unwrap();
        fs::create_dir(&parent_path).unwrap();
        fs::write(parent_path.join("output.json"), output).unwrap();

        let input_kwargs = vec![InputKwarg {
            key: String::from("a"),
            value: Value::String(String::from("v")),
        }];

        let nodemap = HashMap::from([(
            String::from("0"),
            Node {
                uid: String::from("0"),
                status: JobStatus::ReadyForSubmission,
                parents: vec![Parent {
                    uid: String::from("1"),
                    parent_type: ParentType::Output {
                        key: String::from("b"),
                    },
                }],
                children: Vec::new(),
                behavior: NodeBehavior::TaskNode {
                    fname: String::from("foo"),
                    caching: true,
                    try_num: 1,
                    retries: 0,
                    launch_script: String::from("script.sh"),
                    input_kwargs: input_kwargs.clone(),
                },
            },
        )]);

        let state = LocalDirState::new(pipeline_dir);
        let handler = InputDataHandler {
            uid: "0",
            state: &state,
        };

        let input_data = handler.read_input_data(&nodemap, &input_kwargs).unwrap();
        let expected = HashMap::from([
            (String::from("b"), Value::Bool(true)),
            (String::from("a"), Value::String(String::from("v"))),
        ]);

        assert_eq!(input_data, expected);
    }
}
