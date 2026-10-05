// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use crate::engine::context::Ctx;
use crate::engine::polling::Poller;
use crate::engine::submission::{self, inline};
use crate::model::nodes::{Node, Task};
use crate::model::responses::{SacctResponse, SqueueResponse};
use crate::model::schemas::ExecMode;
use crate::model::status::JobType::Slurm;
use crate::model::status::{Completed, Failed, JobType, Status};
use crate::settings::Cfg;
use log;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;

pub struct SlurmPoller<'a> {
    pub cfg: &'a Cfg,
    missing_jobs: HashMap<usize, usize>,
}

impl<'a> Poller for SlurmPoller<'a> {
    fn poll(&mut self, nodes: &[Node], ctx: &mut Ctx) {
        if ctx.slurm_jobs.len() == 0 {
            return;
        }

        let mut poll_result = poll_slurm(&ctx.slurm_jobs);
        self.handle_missing_jobs(&mut poll_result, ctx);
        for (uid, status) in poll_result {
            if let Node::Task(task) = &nodes[uid] {
                self.handle_new_slurm_status(task, status, ctx);
            }
        }
    }
}

impl<'a> SlurmPoller<'a> {
    pub fn new(cfg: &'a Cfg) -> Self {
        Self {
            cfg,
            missing_jobs: HashMap::new(),
        }
    }

    fn handle_new_slurm_status(&mut self, task: &Task, status: Status, ctx: &mut Ctx) {
        match &status {
            Status::Pending(JobType::Slurm(job_id)) => {
                ctx.slurm_jobs.push((task.uid, job_id.to_string()));
                if !matches!(&ctx.statuses[task.uid], Status::Pending(_)) {
                    ctx.set_status(task.uid, status);
                }
            }
            Status::Running(JobType::Slurm(job_id)) => {
                ctx.slurm_jobs.push((task.uid, job_id.to_string()));
                if !matches!(&ctx.statuses[task.uid], Status::Running(_)) {
                    ctx.set_status(task.uid, status);
                }
            }
            Status::Failed(failure) => {
                log::error!("Task - '{}' failed", task.uid);
                submission::handle_retries(task, ctx, failure.clone())
            }
            Status::Completed(Completed::Job(job_type)) => {
                let job = job_type.clone();
                ctx.set_status(task.uid, status);
                ctx.running_cacheable.remove(&task.name);
                ctx.updated.push_back(task.uid);
                if let ExecMode::Ext = task.mode
                    && let Err(e) = inline::submit_save_ext_output(task, &self.cfg)
                {
                    log::error!("Task {}: Failed to save external output - {e}", task.uid);
                    ctx.set_status(task.uid, Status::Failed(Failed::Job(job)));
                    return;
                }
                if task.cache
                    && let Err(e) = inline::submit_save_cache(task, &self.cfg)
                {
                    log::error!("Task {}: Failed to save cache - {e}", task.uid);
                }
            }

            Status::NotSubmitted
            | Status::Skipped
            | Status::ReadyForSubmission
            | Status::Running(_)
            | Status::Pending(_)
            | Status::Completed(_) => {
                log::warn!("Task '{}': status {}", task.uid, status);
                ctx.set_status(task.uid, status);
                ctx.updated.push_back(task.uid);
            }
        }
    }

    fn handle_missing_jobs(&mut self, poll_result: &mut HashMap<usize, Status>, ctx: &mut Ctx) {
        let mut new_jobs = Vec::new();
        for job in ctx.slurm_jobs.drain(..) {
            let nmiss = self.missing_jobs.entry(job.0).or_insert(0);
            if poll_result.contains_key(&job.0) {
                *nmiss = 0;
                continue;
            }

            log::warn!(
                "Slurm response does not contain job {} (task {})",
                job.1,
                job.0
            );

            *nmiss += 1;
            if *nmiss <= self.cfg.grace_period {
                new_jobs.push(job);
                continue;
            }

            log::error!(
                "job {} (task {}) missing for {nmiss} times, marking as failed.",
                job.1,
                job.0
            );
            // Reset in case of retries
            *nmiss = 0;
            let status = Status::Failed(Failed::FailedToContact(Slurm(job.1)));
            poll_result.insert(job.0, status);
        }

        ctx.slurm_jobs = new_jobs;
    }
}

pub fn poll_slurm(jobs: &[(usize, String)]) -> HashMap<usize, Status> {
    let job_ids = jobs
        .iter()
        .map(|x| x.1.as_str())
        .collect::<Vec<&str>>()
        .join(",");

    if job_ids.len() == 0 {
        log::debug!("No running Slurm jobs identified");
        return HashMap::new();
    }

    let mut output: HashMap<String, String> = HashMap::new();
    let sacct_result = run_sacct(&job_ids);
    if let Ok(sacct_response) = sacct_result {
        read_sacct_status(sacct_response, &mut output);
    }

    let squeue_result = run_squeue(&job_ids);
    if let Ok(squeue_response) = squeue_result {
        read_squeue_status(squeue_response, &mut output);
    };

    let mut status_map = HashMap::new();
    for (uid, job_id) in jobs.iter() {
        if let Some(status_str) = output.get(job_id) {
            let status = get_status_from_string(status_str, job_id);
            status_map.insert(*uid, status);
        }
    }
    status_map
}

fn run_sacct(job_ids: &str) -> Result<SacctResponse, Box<dyn Error>> {
    log::debug!("Fetching status from sacct");
    let output = Command::new("sacct")
        .arg("-j")
        .arg(job_ids)
        .arg("--json")
        .output()?;

    let stderr = String::from_utf8(output.stderr);
    if let Ok(msg) = stderr
        && msg.len() > 0
    {
        log::error!("Slurm stderr: {msg}")
    }

    let stdout = String::from_utf8(output.stdout)?;
    let response: SacctResponse = serde_json::from_str(&stdout)?;
    log::debug!("Slurm response:\n{response:?}");
    Ok(response)
}

fn run_squeue(job_ids: &str) -> Result<SqueueResponse, Box<dyn Error>> {
    log::debug!("Fetching status from squeue");
    let output = Command::new("squeue")
        .arg("-j")
        .arg(job_ids)
        .arg("--json")
        .output()?;

    let stderr = String::from_utf8(output.stderr);
    if let Ok(msg) = stderr
        && msg.len() > 0
    {
        log::error!("Slurm stderr: {msg}")
    }

    let stdout = String::from_utf8(output.stdout)?;
    let response: SqueueResponse = serde_json::from_str(&stdout)?;
    log::debug!("squeue response:\n{response:?}");
    Ok(response)
}

fn read_sacct_status(sacct_response: SacctResponse, output: &mut HashMap<String, String>) {
    for mut job in sacct_response.jobs {
        let job_id = job.job_id;
        if job.state.current.len() < 1 {
            log::warn!("sacct returned no current status for job_id {job_id}");
            continue;
        }
        let status = job.state.current.swap_remove(0);
        output.insert(job_id, status);
    }
}

fn read_squeue_status(squeue_response: SqueueResponse, output: &mut HashMap<String, String>) {
    for mut job in squeue_response.jobs {
        let job_id = job.job_id;
        if job.job_state.len() < 1 {
            log::debug!("squeue returned no current status for job_id {job_id}");
            continue;
        }
        let status = job.job_state.swap_remove(0);
        output.insert(job_id, status);
    }
}

fn get_status_from_string(status_string: &str, job_id: &str) -> Status {
    let job_type = JobType::Slurm(job_id.to_string());
    match status_string {
        "COMPLETED" => Status::Completed(Completed::Job(job_type)),
        "CONFIGURING" | "PENDING" => Status::Pending(job_type),
        "RUNNING" | "COMPLETING" => Status::Running(job_type),
        _ => Status::Failed(Failed::Job(job_type)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::context::Job;
    use crate::model::responses::{SacctJob, SacctState, SqueueJob};
    use crate::model::schemas::Cmd;
    use crate::store::workdirs::{DirPaths, FileNames};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn get_poller(cfg: &Cfg) -> SlurmPoller<'_> {
        SlurmPoller {
            cfg,
            missing_jobs: HashMap::new(),
        }
    }

    fn get_tmp_dir() -> PathBuf {
        let path = env::temp_dir().join(Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn get_cfg() -> Cfg {
        let homedir = get_tmp_dir();
        let paths = DirPaths::new(&homedir, "pipeline", "xxx");
        let mut cfg = Cfg::default();
        cfg.homedir = homedir;
        cfg.dagdir = paths.dagdir;
        cfg.cachedir = paths.cachedir;
        cfg.local_cachedir = paths.local_cachedir;
        cfg
    }

    fn get_task(uid: usize) -> Task {
        let mut task = Task::default();
        task.cmd = Cmd::Sbatch;
        task.uid = uid;
        task
    }

    fn get_ctx(nodes: &[Node]) -> Ctx {
        let mut ctx = Ctx::new(nodes).unwrap();
        ctx.try_nums[0] = 1;
        ctx.must_checkpoint = false;
        ctx
    }

    #[test]
    fn test_get_status_from_string() {
        assert!(matches!(
            get_status_from_string("COMPLETED", "1"),
            Status::Completed(Completed::Job(_))
        ));
        assert!(matches!(
            get_status_from_string("PENDING", "1"),
            Status::Pending(JobType::Slurm(_))
        ));
        assert!(matches!(
            get_status_from_string("RUNNING", "1"),
            Status::Running(JobType::Slurm(_))
        ));
    }

    #[test]
    fn test_empty_job_pull() {
        assert!(poll_slurm(&[]).len() == 0)
    }

    #[test]
    fn test_handle_pending_status() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let task = get_task(0);
        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        poller.handle_new_slurm_status(
            &task,
            Status::Pending(JobType::Slurm("123".into())),
            &mut ctx,
        );

        assert!(matches!(
            ctx.statuses[0],
            Status::Pending(JobType::Slurm(_))
        ));
        assert!(ctx.must_checkpoint);
    }

    #[test]
    fn test_constant_pending_status_does_not_trigger_checkpoint() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let task = get_task(0);
        let nodes = [Node::Task(task.clone())];
        let constant_status = Status::Pending(JobType::Slurm("123".into()));
        let mut ctx = get_ctx(&nodes);
        ctx.statuses[0] = constant_status.clone();
        poller.handle_new_slurm_status(&task, constant_status, &mut ctx);

        assert!(matches!(
            ctx.statuses[0],
            Status::Pending(JobType::Slurm(_))
        ));
        assert!(!ctx.must_checkpoint);
    }

    #[test]
    fn test_constant_running_status_does_not_trigger_checkpoint() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let task = get_task(0);
        let nodes = [Node::Task(task.clone())];
        let constant_status = Status::Running(JobType::Slurm("123".into()));
        let mut ctx = get_ctx(&nodes);
        ctx.statuses[0] = constant_status.clone();
        poller.handle_new_slurm_status(&task, constant_status, &mut ctx);

        assert!(matches!(
            ctx.statuses[0],
            Status::Running(JobType::Slurm(_))
        ));
        assert!(!ctx.must_checkpoint);
    }

    #[test]
    fn test_handle_failed_status() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let task = get_task(0);
        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        poller.handle_new_slurm_status(
            &task,
            Status::Failed(Failed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );
        println!("{}", ctx.statuses[0]);
        assert!(matches!(ctx.statuses[0], Status::Failed(_)));
        assert!(ctx.must_checkpoint);
    }

    #[test]
    fn test_handle_failed_status_with_retry() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let mut task = get_task(0);
        task.retries = 1;

        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        poller.handle_new_slurm_status(
            &task,
            Status::Failed(Failed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );

        let job = ctx.jobs.pop_front().unwrap();
        assert!(matches!(job, Job::Task(0)));
        assert!(ctx.must_checkpoint);
    }

    #[test]
    fn test_handle_completed_with_caching() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);
        let mut task = get_task(0);
        task.cache = true;

        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        let src_dir = cfg.dagdir.join(task.uid.to_string());

        fs::create_dir_all(&src_dir).unwrap();
        fs::write(src_dir.join(FileNames::Input.as_str()), "").unwrap();
        fs::write(src_dir.join(FileNames::Output.as_str()), "").unwrap();
        fs::write(src_dir.join(FileNames::Meta.as_str()), "").unwrap();

        poller.handle_new_slurm_status(
            &task,
            Status::Completed(Completed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );

        let cachedir = cfg
            .local_cachedir
            .join(&task.pipeline_name)
            .join(&task.name);

        assert!(cachedir.exists());
        assert!(ctx.must_checkpoint);
    }

    #[test]
    fn test_handle_completed_ext() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);
        let mut task = get_task(0);
        task.mode = ExecMode::Ext;
        task.cache = false;

        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        let taskdir = cfg.dagdir.join(task.uid.to_string());
        fs::create_dir_all(&taskdir).unwrap();

        poller.handle_new_slurm_status(
            &task,
            Status::Completed(Completed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );

        let output = taskdir.join(FileNames::Output.as_str());
        assert!(output.exists());
        assert!(ctx.must_checkpoint);
    }

    #[test]
    fn test_parse_sacct() {
        let response = SacctResponse {
            jobs: vec![
                SacctJob {
                    job_id: "123".into(),
                    state: SacctState {
                        current: vec!["COMPLETED".into(), "???".into()],
                    },
                },
                SacctJob {
                    job_id: "456".into(),
                    state: SacctState {
                        current: vec!["FAILED".into()],
                    },
                },
                SacctJob {
                    job_id: "789".into(),
                    state: SacctState { current: vec![] },
                },
            ],
        };

        let mut output = HashMap::new();
        read_sacct_status(response, &mut output);

        let status_123 = output.get("123").unwrap();
        let status_456 = output.get("456").unwrap();
        let status_789 = output.get("789");
        assert_eq!(status_123, "COMPLETED");
        assert_eq!(status_456, "FAILED");
        assert!(status_789.is_none());
    }

    #[test]
    fn test_parse_squeue() {
        let response = SqueueResponse {
            jobs: vec![
                SqueueJob {
                    job_id: "123".into(),
                    job_state: vec!["COMPLETED".into(), "???".into()],
                },
                SqueueJob {
                    job_id: "456".into(),
                    job_state: vec!["FAILED".into()],
                },
                SqueueJob {
                    job_id: "789".into(),
                    job_state: vec![],
                },
            ],
        };

        let mut output = HashMap::new();
        read_squeue_status(response, &mut output);

        let status_123 = output.get("123").unwrap();
        let status_456 = output.get("456").unwrap();
        let status_789 = output.get("789");
        assert_eq!(status_123, "COMPLETED");
        assert_eq!(status_456, "FAILED");
        assert!(status_789.is_none());
    }
}
