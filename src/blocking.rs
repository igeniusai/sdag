use crate::model::nodes::{Branch, OneOf, Task};
use crate::model::schemas::{Parent, ParentKind, TaskOutput};
use crate::settings::Cfg;
use crate::state;
use crate::workdirs;
use log;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub fn submit_validate_cache(task: &Task, cfg: &Cfg) -> bool {
    if !task.cache {
        return false;
    }

    let dst_path = cfg.dagdir.join(task.uid.to_string());
    let input = match state::read_input_from_parents(task, &cfg.dagdir) {
        Err(e) => {
            log::error!("Task {}: Failed to read input data - {e}", task.uid);
            return false;
        }
        Ok(data) => data,
    };

    log::debug!("Task {}: Validating local cache", task.uid);
    let cache_path = workdirs::get_task_cache_path(task, cfg);
    let res = validate_cache(task.uid, &input, &cache_path, &dst_path, &task.cache_ignore);
    if res && let Err(e) = state::touch_cached_task_dir(&cache_path) {
        log::error!(
            "Task {}: Failed to update the cache modified date - {e}",
            task.uid
        );
    }
    res
}

pub fn submit_save_cache(task: &Task, cfg: &Cfg) -> io::Result<u64> {
    let src_path = cfg.dagdir.join(task.uid.to_string());
    let base_dst_path = workdirs::get_task_cache_path(task, cfg);
    find_and_save_cache(&src_path, &base_dst_path, task.cache_size)
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
    match compare_input_with_cache(&input, &cache_path, cache_ignore) {
        Err(e) => {
            log::info!("Task {uid}: Cache validation failed - {e}");
            false
        }
        Ok(None) => {
            log::info!("Task {uid}: Cached data doesn't match the input");
            false
        }
        Ok(Some(path)) => {
            log::info!("Task {uid}: Cached data matches input");
            if let Err(e) = state::copy_task_data(&path, dst_path) {
                log::error!("Task {uid}: Failed to save cached task data - {e}");
                return false;
            }
            true
        }
    }
}

pub fn compare_input_with_cache(
    input: &HashMap<String, Value>,
    cache_path: &Path,
    cache_ignore: &[String],
) -> io::Result<Option<PathBuf>> {
    for entry in fs::read_dir(cache_path)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        if let Ok(true) = compare_single_cache(&path, input, cache_ignore) {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn compare_single_cache(
    path: &Path,
    input: &HashMap<String, Value>,
    cache_ignore: &[String],
) -> io::Result<bool> {
    let mut cached_input = state::read_input(path)?;
    replace_cache_ignored_value(&mut cached_input, input, cache_ignore);

    let path_str = path.to_string_lossy();
    if *input != cached_input {
        log::info!("Cache {path_str} invalidated as input doesn't match the cached one.");
        log::debug!("input:\n{input:?};\ncached input:\n{cached_input:?}");
        return Ok(false);
    }

    let output = state::read_output(path)?;
    for artifact in &output.artifacts {
        if !artifact.path.exists() {
            log::info!(
                "Cache {path_str}: Artifact {} does not exist, cache validation failed",
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

fn find_and_save_cache(
    src_path: &Path,
    base_dst_path: &Path,
    cache_size: usize,
) -> io::Result<u64> {
    let hash = Uuid::new_v4().to_string();
    let dst_path = base_dst_path.join(hash);
    delete_old_cache(base_dst_path, cache_size)?;
    state::copy_task_data(&src_path, &dst_path)
}

fn delete_old_cache(base_dst_path: &Path, cache_size: usize) -> io::Result<()> {
    if !base_dst_path.is_dir() {
        return Ok(());
    }

    let subfolders = workdirs::get_subfolder_creation_dates(base_dst_path)?;
    let nsubfolders = subfolders.len();
    log::debug!("Currenct cache size: '{}'", nsubfolders);

    let ndel = 1 + nsubfolders as i64 - cache_size as i64;
    if cache_size > 0 && ndel > 0 {
        log::debug!("Number of cache folders to be deleted: {}", ndel);
        for (dir, _) in &subfolders[..ndel as usize] {
            log::debug!("Removing '{}'", dir.to_string_lossy());
            fs::remove_dir_all(dir)?;
        }
    }
    Ok(())
}
