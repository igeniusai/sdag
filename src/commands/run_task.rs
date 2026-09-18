use crate::model::nodes::{Node, Task};
use crate::model::schemas::{Cmd, DAGMeta};
use crate::settings::{self, Cfg};
use crate::store::workdirs;
use crate::submission::nonblocking::{submit_local_blocking, submit_slurm};
use chrono::Local;
use log;
use serde_json::{self, Value};
use std::collections::HashMap;
use uuid::Uuid;

pub fn run_task(
    task_serialized: &str,
    slurm_grace_period: usize,
    max_concurrent_runs: usize,
    time_between_polls: u64,
    log_level: &str,
) {
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let task: Task = serde_json::from_str(task_serialized).expect("Failed to parse task");
    let meta = DAGMeta {
        pipeline_name: task.pipeline_name.clone(),
        hash: get_runtask_hash(&task.name),
        timestamp: format!("{}", Local::now().format("%Y-%m-%dT%H:%M:%S")),
        extra: Value::Null,
        import_path: task.pipeline_name.clone(), // TODO
        kwargs: HashMap::new(),
    };

    let cfg = Cfg::new(
        &homedir,
        &meta,
        &log_level,
        1,
        time_between_polls,
        slurm_grace_period,
        max_concurrent_runs,
        false,
    );
    let nodes = vec![Node::Task(task.clone())];
    workdirs::create_dir_structure(&cfg, &nodes).expect("Failed to create directories");
    submit_job(&task, &cfg, &meta);
}

fn submit_job(task: &Task, cfg: &Cfg, meta: &DAGMeta) {
    match &task.cmd {
        Cmd::Sbatch => match submit_slurm(&task, 1, &cfg, &meta) {
            Ok(job_id) => log::info!("Submitted job '{job_id}'"),
            Err(e) => log::error!("Job submission failed - {e}"),
        },
        Cmd::Bash => match submit_local_blocking(&task, 1, &cfg, &meta) {
            Ok(output) => {
                if !output.status.success() {
                    log::error!("Job terminated with non-zero exit status");
                } else {
                    log::info!("Job successfully completed.");
                }
            }
            Err(e) => log::error!("Job failed - {e}"),
        },
    }
}

fn get_runtask_hash(task_name: &str) -> String {
    let long_hash = Uuid::new_v4().to_string();
    let short_hash: String = long_hash.chars().take(8).collect();
    format!("{}-{}", task_name, short_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtask_hash() {
        let task_name = "name".into();
        let runtask_hash = get_runtask_hash(task_name);
        // name + - + <8-letter-hash>
        assert_eq!(runtask_hash.chars().count(), 13);
    }
}
