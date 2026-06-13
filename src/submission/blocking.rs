use crate::nodes::{Branch, OneOf, Task};
use crate::schemas::{Parent, ParentKind, TaskOutput};
use crate::settings::Cfg;
use crate::state;
use log;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::path::Path;

pub fn submit_validate_cache(task: &Task, cfg: &Cfg) -> bool {
    let dst_path = cfg.dagdir.join(task.uid.to_string());
    let input = match state::read_input_from_parents(task, &cfg.dagdir) {
        Err(e) => {
            log::error!("Task {}: Failed to read input data - {e}", task.uid);
            return false;
        }
        Ok(data) => data,
    };

    if task.cache_local {
        log::info!("Task {}: Validating local cache", task.uid);
        let cache_path = cfg
            .local_cachedir
            .join(&task.pipeline_name)
            .join(&task.name);
        if validate_cache(task.uid, &input, &cache_path, &dst_path, &task.cache_ignore) {
            return true;
        }
        log::info!("Task {}: Local cache validation failed", task.uid);
    }

    if task.cache {
        log::info!("Task {}: Validating global cache", task.uid);
        let cache_path = cfg.cachedir.join(&task.name);
        return validate_cache(task.uid, &input, &cache_path, &dst_path, &task.cache_ignore);
    }

    false
}

pub fn submit_save_cache(task: &Task, cfg: &Cfg) -> io::Result<()> {
    let src_path = cfg.dagdir.join(task.uid.to_string());
    if task.cache {
        let dst_path = cfg.cachedir.join(&task.name);
        state::copy_task_data(&src_path, &dst_path)?;
    }

    if task.cache_local {
        let dst_path = cfg
            .local_cachedir
            .join(&task.pipeline_name)
            .join(&task.name);
        state::copy_task_data(&src_path, &dst_path)?;
    }
    Ok(())
}

pub fn submit_branch(branch: &Branch, cfg: &Cfg) -> Result<bool, String> {
    let target = find_branch_target(&branch.parents)?; //branch.parent.uid;
    let path = cfg.dagdir.join(target.to_string());
    let output = state::read_output(&path)
        .map_err(|e| format!("Failed to read task '{target}' output - {e}"))?;
    match output.output {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) => Ok(false),
        _ => {
            let msg = format!("Target {}, output is not Boolean", target);
            Err(msg)
        }
    }
}

pub fn submit_oneof(oneof: &OneOf, target: usize, cfg: &Cfg) -> io::Result<u64> {
    let src_dir = cfg.dagdir.join(target.to_string());
    let dst_dir = cfg.dagdir.join(oneof.uid.to_string());
    state::copy_task_data(&src_dir, &dst_dir)
}

pub fn submit_save_ext_output(task: &Task, cfg: &Cfg) -> io::Result<()> {
    let path = cfg.dagdir.join(task.uid.to_string());
    let output = TaskOutput {
        output: serde_json::Value::Null,
        artifacts: task.artifacts.clone(),
    };
    state::save_task_output(&path, &output)
}

pub fn validate_cache(
    uid: usize,
    input: &HashMap<String, Value>,
    cache_path: &Path,
    dst_path: &Path,
    cache_ignore: &[String],
) -> bool {
    match compare_input_with_cache(uid, &input, &cache_path, cache_ignore) {
        Err(e) => {
            log::info!("Task {uid}: Cache validation failed - {e}");
            false
        }
        Ok(false) => {
            log::info!("Task {uid}: Cached data doesn't match the input");
            false
        }
        Ok(true) => {
            log::info!("Task {uid}: Cached data matches input");
            if let Err(e) = state::copy_task_data(cache_path, dst_path) {
                log::error!("Task {uid}: Failed to save cached task data - {e}");
                return false;
            }
            true
        }
    }
}

pub fn compare_input_with_cache(
    uid: usize,
    input: &HashMap<String, Value>,
    cache_path: &Path,
    cache_ignore: &[String],
) -> io::Result<bool> {
    let mut cached_input = state::read_input(cache_path)?;
    replace_cache_ignored_value(&mut cached_input, input, cache_ignore);

    if *input != cached_input {
        log::info!("Task {uid}: Cache invalidated as input doesn't match the cached one.");
        log::debug!("Task {uid}:\ninput:\n{input:?};\ncached input:\n{cached_input:?}");
        return Ok(false);
    }

    let output = state::read_output(cache_path)?;
    for artifact in &output.artifacts {
        if !artifact.path.exists() {
            log::info!(
                "Task {uid}: Artifact {} does not exist, cache validation failed",
                artifact.path.to_string_lossy()
            );
            return Ok(false);
        }
    }
    Ok(true)
}

fn find_branch_target(parents: &[Parent]) -> Result<usize, String> {
    for parent in parents {
        if let ParentKind::Output { .. } = parent.kind {
            return Ok(parent.uid);
        }
    }

    Err("Failed to identify the target task".into())
}

fn replace_cache_ignored_value(
    cached_input: &mut HashMap<String, Value>,
    input: &HashMap<String, Value>,
    cache_ignore: &[String],
) {
    for key in cache_ignore {
        if let Some(value) = input.get(key) {
            log::debug!("Inserting excluded cache value {key}");
            cached_input.insert(key.clone(), value.clone());
        }
    }
}
