use crate::backend::{Backend, SchedulerBackend};
use crate::checkpoint::Checkpointer;
use crate::model::{DAG, DAGMetadata, JobStatus, Node};
use crate::startup;
use crate::state::{LocalDirState, StateManager};
use log;

pub fn kill_running_jobs(pipeline_name: String) {
    let home_dir = startup::find_home_dir()
        .map_err(|e| log::error!("{e}"))
        .unwrap();

    let state = LocalDirState::new(&home_dir, &pipeline_name);
    log::info!("Working directory: '{:?}'", state.get_pipeline_dir());

    // Most metadata is mocked here
    let checkpointer = Checkpointer {
        meta: DAGMetadata {
            name: pipeline_name.clone(),
            creation_dt: String::new(),
        },
        restart: false,
    };

    let dag = checkpointer
        .load_checkpoint(&state)
        .map_err(|e| log::error!("Failed load checkpoint: {e}"))
        .unwrap();

    let pipeline_name = pipeline_name.clone();
    let backend = SchedulerBackend::new(&pipeline_name, &state);
    let jobs = find_running_jobs(&dag);
    backend
        .kill_jobs(&jobs)
        .map_err(|e| log::error!("Failed to cancel jobs: {e}"))
        .unwrap();
}

fn find_running_jobs(dag: &DAG<Node>) -> Vec<&str> {
    dag.nodes
        .iter()
        .filter_map(|n| {
            if let JobStatus::Running(job_id) = &n.status {
                Some(job_id.as_str())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Cmd, ExecMode, Node, NodeBehavior, Task};

    #[test]
    fn find_job_ids() {
        let dag = DAG {
            meta: DAGMetadata {
                name: "hello".to_string(),
                creation_dt: String::new(),
            },
            nodes: vec![
                Node {
                    uid: "0".to_string(),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::Running("1234".to_string()),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: "fn".to_string(),
                        name: String::from("fname"),
                        caching: false,
                        mode: ExecMode::Ext,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 0,
                        launch_script: "script.sh".to_string(),
                        input_kwargs: Vec::new(),
                    }),
                },
                Node {
                    uid: "1".to_string(),
                    output_artifacts: Vec::new(),
                    children: Vec::new(),
                    parents: Vec::new(),
                    status: JobStatus::NotSubmitted,
                    behavior: NodeBehavior::RootNode,
                },
                Node {
                    uid: "2".to_string(),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::Running("5678".to_string()),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: "fn".to_string(),
                        name: String::from("fname"),
                        caching: false,
                        mode: ExecMode::Ext,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 0,
                        launch_script: "script.sh".to_string(),
                        input_kwargs: Vec::new(),
                    }),
                },
            ],
        };

        let mut jobs = find_running_jobs(&dag);
        jobs.sort();
        assert_eq!(jobs, vec!["1234", "5678"]);
    }
}
