use crate::nodes::{Node, Task};
use crate::schemas::{Artifact, ParentKind};
use crate::settings::{self, Cfg};
use crate::state;
use crate::submission::blocking;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::path::PathBuf;

use tabled::{
    Table, Tabled,
    settings::{Alignment, Style, object::Columns, object::Segment},
};

#[derive(Tabled)]
struct TaskDescription<'a> {
    uid: usize,
    /// Task status.
    name: &'a str,
    /// Task pipeline.
    pipeline: &'a str,
    /// Cached
    cached: bool,
    /// Artifacts
    artifacts: String,
    /// Input kwargs
    kwargs: String,
}

pub fn describe_pipeline(pipeline_path: &str) {
    let pipeline_path = PathBuf::from(pipeline_path);
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let dag = state::read_dag(&pipeline_path).expect("Failed to read DAG - {e}");
    let cfg = Cfg::new(&homedir, &dag.meta, 1, 0, true, false);
    let mut descriptions = vec![];

    for node in &dag.nodes {
        if let Node::Task(task) = node {
            let description = get_description(task, &cfg);
            descriptions.push(description);
        }
    }

    let mut table = Table::new(descriptions);
    table.with(Style::modern());
    table.modify(Columns::first(), Alignment::right());
    table.modify(Segment::all(), Alignment::center_vertical());
    println!("{table}");
}

fn get_description<'a>(task: &'a Task, cfg: &Cfg) -> TaskDescription<'a> {
    let input_kwargs = state::get_static_input(&task.kwargs, &task.parents);
    let kwargs =
        serde_json::to_string_pretty(&input_kwargs).unwrap_or("[failed to parse]".to_string());
    let cached = check_caching(&task, &cfg, &input_kwargs);
    let artifacts = get_artifacts(&task.artifacts);

    TaskDescription {
        uid: task.uid,
        name: &task.name,
        pipeline: &task.pipeline_name,
        cached,
        artifacts,
        kwargs,
    }
}

fn check_caching(task: &Task, cfg: &Cfg, input: &HashMap<String, Value>) -> bool {
    let is_dynamic = task
        .parents
        .iter()
        .any(|p| matches!(p.kind, ParentKind::Output { .. }));

    if is_dynamic {
        return false;
    }

    if task.cache_local {
        let cache_path = cfg
            .local_cachedir
            .join(&task.pipeline_name)
            .join(&task.name);
        let path_str = cache_path.to_string_lossy();
        match blocking::compare_input_with_cache(task.uid, input, &cache_path) {
            Ok(true) => return true,
            Ok(false) => {
                log::info!(
                    "Task {}: Local cache at '{path_str}' doesn't match",
                    task.uid
                )
            }
            Err(e) => {
                log::warn!(
                    "Task {}: Failed to compare local cache at '{path_str}' - {e}",
                    task.uid
                );
            }
        }
    }

    if task.cache {
        let cache_path = cfg.cachedir.join(&task.name);
        let path_str = cache_path.to_string_lossy();
        match blocking::compare_input_with_cache(task.uid, input, &cache_path) {
            Ok(true) => return true,
            Ok(false) => {
                log::info!("Task {}: cache at '{path_str}' doesn't match", task.uid)
            }
            Err(e) => {
                log::warn!(
                    "Task {}: Failed to compare cache at '{path_str}' - {e}",
                    task.uid
                );
            }
        }
    }

    false
}

fn get_artifacts(artifacts: &[Artifact]) -> String {
    artifacts
        .iter()
        .map(|a| a.name.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}
