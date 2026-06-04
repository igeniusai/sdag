use crate::nodes::{Node, Task};
use crate::schemas::{Cmd, DAGMeta};
use crate::settings::{self, Cfg};
use crate::submission::nonblocking::{submit_local_blocking, submit_slurm};
use crate::workdirs;
use chrono::Local;
use log;
use serde_json::{self, Value};
use uuid::Uuid;

pub fn run_task(task_serialized: &str) {
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let task: Task = serde_json::from_str(task_serialized).expect("Failed to parse task");
    let meta = DAGMeta {
        pipeline_name: task.pipeline_name.clone(),
        hash: format!("task-{}-{}", task.name, Uuid::new_v4().to_string()),
        timestamp: format!("{}", Local::now().format("%Y-%m-%dT%H:%M:%S")),
        extra: Value::Null,
        import_path: String::new(), // TODO
    };

    let is_local = matches!(task.cmd, Cmd::Bash);
    let cfg = Cfg::new(&homedir, &meta, 1, 0, is_local, task.debug);
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
