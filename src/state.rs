use crate::context::Ctx;
use crate::nodes::Node;
use crate::nodes::Task;
use crate::schemas::{Checkpoint, DAG, DAGMeta, Kwarg, Parent, ParentKind, TaskOutput};
use crate::settings::Cfg;
use crate::workdirs::FileNames;
use serde::Deserialize;
use serde_json::{Deserializer, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// Parse the DAG from the input JSON.
pub fn read_dag(path: &Path) -> Result<DAG, String> {
    log::info!("Reading DAG from {}", path.to_string_lossy());
    let buf = fs::read_to_string(path).map_err(|e| format!("Failed to read DAG - {e}"))?;
    serde_json::from_str(&buf).map_err(|e| format!("Failed to parse DAG - {e}"))
}

/// Parse the DAG from the input JSON.
pub fn read_chekpoint(path: &Path) -> Result<Checkpoint<'static>, Box<dyn Error>> {
    log::info!("Reading checkpoint from path {}", path.to_string_lossy());
    let file_path = path.join(FileNames::Checkpoint.as_str());
    let file = fs::File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut deserializer = Deserializer::from_reader(reader);
    let checkpoint = Checkpoint::deserialize(&mut deserializer)?;

    Ok(checkpoint)
}

pub fn copy_dag_in_dagdir(path: &Path, dagdir: &Path) -> io::Result<u64> {
    let dst = dagdir.join(FileNames::DAG.as_str());
    fs::copy(path, dst)
}

pub fn save_checkpoint(cfg: &Cfg, meta: &DAGMeta, nodes: &[Node], ctx: &Ctx) -> io::Result<()> {
    let checkpoint = Checkpoint {
        cfg: Cow::Borrowed(cfg),
        meta: Cow::Borrowed(meta),
        statuses: Cow::Borrowed(&ctx.statuses),
        nodes: Cow::Borrowed(nodes),
        try_nums: Cow::Borrowed(&ctx.try_nums),
    };

    let path = checkpoint.cfg.dagdir.join(FileNames::Checkpoint.as_str());
    log::debug!("Saving checkpoint to path: {}", path.to_string_lossy());
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    fs::write(&path, &checkpoint_json)
}

/// Read static and parent input data
pub fn read_input_from_parents(task: &Task, dagdir: &Path) -> io::Result<HashMap<String, Value>> {
    let mut input_data = get_static_input(&task.kwargs, &task.parents);
    for parent in &task.parents {
        if let ParentKind::Output { key } = &parent.kind {
            let value = get_dynamic_input(parent.uid, dagdir)?;
            input_data.insert(key.to_string(), value);
        }
    }
    Ok(input_data)
}

pub fn read_input(path: &Path) -> io::Result<HashMap<String, Value>> {
    let input_file = FileNames::Input.as_str();
    let data = fs::read_to_string(path.join(input_file))?;
    serde_json::from_str(&data).map_err(|e| e.into())
}

pub fn save_input(input: &HashMap<String, Value>, path: &Path) -> io::Result<()> {
    let contents = serde_json::to_string(input)?;
    fs::write(path, contents)
}

pub fn read_output(path: &Path) -> io::Result<TaskOutput> {
    let output_path = path.join(FileNames::Output.as_str());
    let output = fs::read_to_string(&output_path)?;
    serde_json::from_str(&output).map_err(|e| e.into())
}

pub fn save_task_output(path: &Path, output: &TaskOutput) -> io::Result<()> {
    let output_path = path.join(FileNames::Output.as_str());
    let contents = serde_json::to_string(output)?;
    fs::write(output_path, contents)
}

pub fn copy_task_data(src: &Path, dst: &Path) -> io::Result<u64> {
    fs::create_dir_all(dst)?;
    let output_file = FileNames::Output.as_str();
    let output_from = src.join(output_file);
    let output_to = dst.join(output_file);
    fs::copy(&output_from, output_to)?;

    let input_file = FileNames::Input.as_str();
    let input_from = src.join(input_file);
    let input_to = dst.join(input_file);
    fs::copy(&input_from, input_to)?;

    let meta_file = FileNames::Meta.as_str();
    let meta_from = src.join(meta_file);
    let meta_to = dst.join(meta_file);
    fs::copy(&meta_from, meta_to)
}

pub fn rm_kill_file_if_present(dagdir: &Path) {
    let path = dagdir.join(FileNames::Kill.as_str());
    let _ = fs::remove_file(path);
}

pub fn is_scheduler_killed(dagdir: &Path) -> bool {
    let path = dagdir.join(FileNames::Kill.as_str());
    path.exists()
}

pub fn create_kill_file(dagdir: &Path) -> io::Result<()> {
    let path = dagdir.join(FileNames::Kill.as_str());
    fs::File::create(path)?;
    Ok(())
}

pub fn save_script(script: &str, path: &Path) -> io::Result<()> {
    fs::write(path, script)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o770))
}

pub fn clear_local_cache(task_name: &str, pipeline_name: &str, homedir: &Path) -> io::Result<()> {
    log::info!("Removing task '{task_name}' (pipeline '{pipeline_name}') cache");
    let path = homedir
        .join(".cache")
        .join("local")
        .join(pipeline_name)
        .join(task_name);

    log::debug!("Cache path: '{}'", path.to_string_lossy());
    if !path.is_dir() {
        log::warn!(
            "Local cache for task '{}' (pipeline '{}') not found",
            task_name,
            pipeline_name
        );
        return Ok(());
    } else {
        log::info!(
            "Deleting task '{}' (pipeline '{}') local cache",
            task_name,
            pipeline_name
        );
        fs::remove_dir_all(path)
    }
}

pub fn clear_global_cache(
    task_name: &str,
    allow_full_prune: bool,
    homedir: &Path,
) -> io::Result<()> {
    log::info!("Removing task '{task_name}' global cache");
    let cache_dir = homedir.join(".cache");
    let path = cache_dir.join("global").join(task_name);
    log::debug!("Cache path: '{}'", path.to_string_lossy());

    if !path.is_dir() {
        if task_name == "all" && allow_full_prune && cache_dir.is_dir() {
            log::info!("Deleting the whole cache");
            return fs::remove_dir_all(&cache_dir);
        } else {
            log::warn!("Global cache for task '{}' not found", task_name);
            return Ok(());
        }
    }

    log::info!("Deleting task '{}' global cache", task_name);
    fs::remove_dir_all(path)
}

/// Add the static input to the input data
pub fn get_static_input(kwargs: &[Kwarg], parents: &[Parent]) -> HashMap<String, Value> {
    let mut static_input: HashMap<String, Value> = kwargs
        .iter()
        .map(|k| (k.key.to_string(), k.value.clone()))
        .collect();

    for parent in parents {
        if let ParentKind::Artifact { key, path, .. } = &parent.kind {
            let path_str = path.to_string_lossy().into_owned();
            static_input.insert(key.to_string(), Value::String(path_str));
        }
    }

    static_input
}

/// Create the output and error log directories
pub fn create_output_and_error_log_dirs(output: &str, error: &str) {
    let path_out = PathBuf::from(&output);
    let path_err = PathBuf::from(&error);
    if let Some(output_dir) = path_out.parent() {
        if let Err(e) = fs::create_dir_all(output_dir) {
            log::warn!("Failed to create output log directory: {e}");
        }
    }
    if let Some(error_dir) = path_err.parent() {
        if let Err(e) = fs::create_dir_all(error_dir) {
            log::warn!("Failed to create error log directory: {e}");
        }
    }
}

/// Add the dynamic input to the input data
fn get_dynamic_input(uid: usize, dagdir: &Path) -> io::Result<Value> {
    let path = dagdir.join(uid.to_string());
    let output = read_output(&path)?;
    Ok(output.output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::End;
    use crate::schemas::{Cmd, ExecMode, Script, ScriptPath, SlurmOverride};
    use serde_json::{Value, json};
    use std::env;
    use std::path::PathBuf;
    use std::time::Duration;
    use uuid::Uuid;

    fn get_tmp_dir() -> PathBuf {
        let path = env::temp_dir().join(Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    #[should_panic]
    fn test_read_missing_dag() {
        let path = get_tmp_dir();
        read_dag(&path).unwrap();
    }

    #[test]
    fn test_read_dag() {
        let dag_str = r#"{
    "meta": {
        "pipeline_name": "hello_world",
        "hash": "e",
        "import_path": "path.to.module:pipeline",
        "timestamp": "1900-01-01T09:20:20",
        "extra": {"extra_field": "hello"},
        "kwargs": {"a": 1, "b": true}
    },
    "nodes": [
        {
            "uid": 0,
            "kind": "root",
            "pipeline_name": "pipeline",
            "parents": [],
            "artifacts": []
        }
    ]
}"#;
        let path = &get_tmp_dir();
        let dag_path = path.join("dag.json");
        fs::write(&dag_path, dag_str).unwrap();
        let dag = read_dag(&dag_path).unwrap();
        let exp_kwargs =
            HashMap::from([("a".to_string(), json![1]), ("b".to_string(), json![true])]);
        assert_eq!(dag.meta.kwargs, exp_kwargs);
    }

    #[test]
    fn test_copy_dag_in_dagdir() {
        let path = get_tmp_dir();
        let dag_str = r#"{
    "meta": {
        "pipeline_name": "hello_world",
        "hash": "e",
        "timestamp": "1900-01-01T09:20:20",
        "extra": {"extra_field": "hello"}
    },
    "nodes": [
        {
            "uid": 0,
            "kind": "root",
            "parents": [],
            "artifacts": []
        }
    ]
}"#;
        let src_path = path.join("src_dag.json");
        fs::write(&src_path, dag_str).unwrap();
        copy_dag_in_dagdir(&src_path, &path).unwrap();
        assert!(path.join("dag.json").is_file());
    }

    #[test]
    fn test_write_and_read_checkpoint() {
        let path = get_tmp_dir();
        let cfg = Cfg {
            homedir: path.clone(),
            dagdir: path.clone(),
            cachedir: path.clone(),
            local_cachedir: path.clone(),
            timestamp: "1900-01-01T09:20:20".into(),
            grace_period: 3,
            max_dagdirs: 1,
            max_concurrency: 5,
            sleep_time: Duration::from_secs(2),
        };

        let meta = DAGMeta {
            pipeline_name: "name".into(),
            hash: "xxxx".into(),
            timestamp: "1900-01-01T09:20:20".into(),
            extra: Value::Null,
            import_path: String::new(),
            kwargs: HashMap::new(),
        };
        let nodes = vec![Node::End(End {
            uid: 0,
            pipeline_name: "pipeline".into(),
            parents: vec![],
            children: vec![],
            artifacts: vec![],
        })];
        let ctx = Ctx::new(&nodes).unwrap();
        save_checkpoint(&cfg, &meta, &nodes, &ctx).unwrap();
        let ckpt = read_chekpoint(&path).unwrap();

        assert_eq!(ckpt.cfg.into_owned(), cfg);
        assert_eq!(ckpt.meta.into_owned(), meta);
        assert_eq!(ckpt.nodes.into_owned(), nodes);
        assert_eq!(ckpt.statuses.into_owned(), ctx.statuses);
        assert_eq!(ckpt.try_nums.into_owned(), ctx.try_nums);
    }

    #[test]
    fn write_and_read_input() {
        let input = HashMap::from([("k".into(), Value::Null)]);
        let path = get_tmp_dir();
        let input_path = path.join("input.json");
        save_input(&input, &input_path).unwrap();
        let input_read = read_input(&path).unwrap();
        assert_eq!(input, input_read);
    }

    #[test]
    fn write_and_read_output() {
        let output = TaskOutput {
            output: Value::Null,
            artifacts: Vec::new(),
        };

        let path = get_tmp_dir();
        save_task_output(&path, &output).unwrap();
        let output_read = read_output(&path).unwrap();
        assert_eq!(output, output_read);
    }

    #[test]
    fn test_copy_task_data() {
        let path = get_tmp_dir();
        let src_path = path.join("src");
        let dst_path = path.join("dst");
        let input_path = src_path.join("input.json");
        let output_path = src_path.join("output.json");
        let meta_path = src_path.join("meta.json");

        fs::create_dir_all(&src_path).unwrap();
        fs::File::create(input_path).unwrap();
        fs::File::create(output_path).unwrap();
        fs::File::create(meta_path).unwrap();

        copy_task_data(&src_path, &dst_path).unwrap();

        assert!(dst_path.join("input.json").is_file());
        assert!(dst_path.join("output.json").is_file());
        assert!(dst_path.join("meta.json").is_file());
    }

    #[test]
    fn create_and_drop_kill_file() {
        let path = get_tmp_dir();
        let kill_file = path.join("kill.lock");

        create_kill_file(&path).unwrap();
        assert!(kill_file.exists());
        rm_kill_file_if_present(&path);
        assert!(!kill_file.exists());
    }

    #[test]
    fn test_script_save() {
        let path = get_tmp_dir();
        let script_path = path.join("script.sh");
        let script = "echo hello";
        save_script(&script, &script_path).unwrap();
        assert!(script_path.is_file());

        let perms = fs::metadata(script_path).unwrap().permissions();
        assert_eq!(perms.mode(), 33272);
    }

    #[test]
    fn test_read_input_from_parents() {
        let path = get_tmp_dir();
        let output: TaskOutput = serde_json::from_value(json!({
            "output": false,
            "artifacts": [],
        }))
        .unwrap();
        let output_path = path.join("2");
        fs::create_dir_all(&output_path).unwrap();
        save_task_output(&output_path, &output).unwrap();

        let task = Task {
            uid: 3,
            parents: vec![
                Parent {
                    uid: 0,
                    kind: ParentKind::Logical,
                },
                Parent {
                    uid: 1,
                    kind: ParentKind::Artifact {
                        key: "artifact".into(),
                        name: "artifact_name".into(),
                        path: PathBuf::from("path/to/artifact"),
                    },
                },
                Parent {
                    uid: 2,
                    kind: ParentKind::Output {
                        key: "output".into(),
                    },
                },
            ],
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: true,
            cache_local: false,
            cache_ignore: vec![],
            mode: ExecMode::Wrap,
            cmd: Cmd::Bash,
            envs: HashMap::new(),
            retries: 0,
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            tags: vec![],
            kwargs: vec![Kwarg {
                key: "key".into(),
                value: Value::Null,
            }],
            artifacts: vec![],
            children: vec![],
            slurm_override: SlurmOverride::new(),
        };

        let expected = HashMap::from([
            ("key".into(), json!(null)),
            ("artifact".into(), json!("path/to/artifact")),
            ("output".into(), json!(false)),
        ]);

        let input = read_input_from_parents(&task, &path).unwrap();
        assert_eq!(input, expected);
    }

    #[test]
    fn create_output_and_error_logdirs() {
        let path = get_tmp_dir();
        let out_dir = path.join("out");
        let err_dir = path.join("err");
        let path_out = out_dir.join("123.log");
        let path_err = err_dir.join("123.log");

        create_output_and_error_log_dirs(&path_out.to_string_lossy(), &path_err.to_string_lossy());

        assert!(out_dir.is_dir());
        assert!(err_dir.is_dir());
    }
}
