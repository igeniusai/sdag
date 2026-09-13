use crate::context::{Ctx, Job};
use crate::nodes::{Node, Task};
use crate::polling::Poller;
use crate::schemas::ExecMode;
use crate::settings::Cfg;
use crate::status::JobType::Slurm;
use crate::status::{Completed, Failed, JobType, Status};
use crate::submission;
use log;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;

pub struct SlurmPoller<'a> {
    pub cfg: &'a Cfg,
    pub nslurm_fails: usize,
    missing_jobs: HashMap<usize, usize>,
}

impl<'a> Poller for SlurmPoller<'a> {
    fn poll(&mut self, nodes: &[Node], ctx: &mut Ctx) {
        if ctx.slurm_jobs.len() == 0 {
            return;
        }

        match poll_slurm(&ctx.slurm_jobs) {
            Ok(mut poll_result) => {
                self.nslurm_fails = 0;
                ctx.slurm_jobs = self.handle_missing_jobs(&mut poll_result, ctx);
                for (uid, status) in poll_result {
                    if let Node::Task(task) = &nodes[uid] {
                        self.handle_new_slurm_status(task, status, ctx);
                    }
                }
            }

            Err(e) => {
                log::error!("Failed to contact Slurm - {e}");
                self.nslurm_fails += 1;
                if self.nslurm_fails >= self.cfg.grace_period {
                    self.mark_all_jobs_as_failed(ctx);
                }
            }
        }
    }
}

impl<'a> SlurmPoller<'a> {
    pub fn new(cfg: &'a Cfg) -> Self {
        Self {
            cfg,
            nslurm_fails: 0,
            missing_jobs: HashMap::new(),
        }
    }

    fn handle_new_slurm_status(&mut self, task: &Task, status: Status, ctx: &mut Ctx) {
        match &status {
            Status::Pending(JobType::Slurm(job_id)) | Status::Running(JobType::Slurm(job_id)) => {
                ctx.slurm_jobs.push((task.uid, job_id.to_string()));
                ctx.statuses[task.uid] = status;
            }
            Status::Failed(failure) => {
                log::error!("Task - '{}' failed", task.uid);
                submission::handle_retries(task, ctx, failure.clone())
            }
            Status::Completed(Completed::Job(job_type)) => {
                let _ = ctx.running_cacheable.remove(&task.name);
                if let ExecMode::Ext = task.mode {
                    let job_type = job_type.clone();
                    ctx.statuses[task.uid] = status;
                    ctx.jobs.push_back(Job::SaveExtOutput(task.uid, job_type));
                    return;
                }

                ctx.statuses[task.uid] = status;
                ctx.updated.push_back(task.uid);
                if task.cache | task.cache_local {
                    ctx.jobs.push_back(Job::SaveCache(task.uid));
                }
            }

            Status::NotSubmitted
            | Status::Skipped
            | Status::ReadyForSubmission
            | Status::Running(_)
            | Status::Pending(_)
            | Status::Completed(_) => {
                log::warn!("Task '{}': status {}", task.uid, status);
                ctx.statuses[task.uid] = status;
                ctx.updated.push_back(task.uid);
            }
        }
    }

    fn mark_all_jobs_as_failed(&mut self, ctx: &mut Ctx) {
        log::error!("Slurm grace period reached, marking all jobs as failed");
        for (uid, job_id) in &ctx.slurm_jobs {
            let job_type = JobType::Slurm(job_id.to_string());
            ctx.statuses[*uid] = Status::Failed(Failed::FailedToContact(job_type));
            ctx.updated.push_back(*uid);
        }
        self.nslurm_fails = 0;
        ctx.slurm_jobs = Vec::new();
    }

    fn handle_missing_jobs(
        &mut self,
        poll_result: &mut HashMap<usize, Status>,
        ctx: &mut Ctx,
    ) -> Vec<(usize, String)> {
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
        new_jobs
    }
}

pub fn poll_slurm(jobs: &[(usize, String)]) -> Result<HashMap<usize, Status>, Box<dyn Error>> {
    let job_ids = jobs
        .iter()
        .map(|x| x.1.as_str())
        .collect::<Vec<&str>>()
        .join(",");

    if job_ids.len() == 0 {
        log::debug!("No running Slurm jobs identified");
        return Ok(HashMap::new());
    }

    let stdout = ask_status_to_slurm(&job_ids)?;
    let mut output = HashMap::new();
    for (uid, job_id) in jobs {
        if let Some(status_string) = parse_slurm_status(job_id, &stdout) {
            log::info!("Slurm job '{job_id}' status: '{status_string}'");
            let status = get_status_from_string(&status_string, job_id);
            output.insert(*uid, status);
        } else {
            log::warn!("Slurm is reachable but it did't return any status");
        }
    }

    Ok(output)
}

fn ask_status_to_slurm(job_ids: &str) -> Result<String, Box<dyn Error>> {
    let output = Command::new("sacct")
        .arg("-j")
        .arg(job_ids)
        .arg("--format")
        .arg("JobID,State")
        .output()?;

    let stderr = String::from_utf8(output.stderr);
    if let Ok(msg) = stderr
        && msg.len() > 0
    {
        log::error!("Slurm stderr: {msg}")
    }

    let stdout = String::from_utf8(output.stdout)?;
    log::debug!("Slurm stdout:\n{stdout}");
    Ok(stdout)
}

fn parse_slurm_status(job_id: &str, stdout: &str) -> Option<String> {
    let matched = format!("{job_id}\\s+(?<status>\\w+)");
    let re = Regex::new(&matched).expect("Failed to create Slurm regex");
    for line in stdout.lines() {
        if let Some(caps) = re.captures(line) {
            return Some(caps["status"].into());
        }
    }
    None
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
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::schemas::{Cmd, DAGMeta, Script, ScriptPath, SlurmOverride};
    use serde_json::Value;

    fn get_meta() -> DAGMeta {
        DAGMeta {
            pipeline_name: "pipe".into(),
            hash: "xxx".into(),
            timestamp: "1920-01-01T09:20:20".into(),
            extra: Value::Null,
            import_path: String::new(),
            kwargs: HashMap::new(),
        }
    }

    fn get_cfg() -> Cfg {
        let meta = get_meta();
        Cfg::new(&PathBuf::from("/a/path"), &meta, 5, 4)
    }

    fn get_poller(cfg: &Cfg) -> SlurmPoller<'_> {
        SlurmPoller {
            cfg,
            nslurm_fails: 3,
            missing_jobs: HashMap::new(),
        }
    }

    fn get_task(uid: usize) -> Task {
        Task {
            uid,
            parents: vec![],
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: false,
            cache_local: false,
            cache_ignore: vec![],
            cache_size: 1,
            mode: ExecMode::Wrap,
            cmd: Cmd::Sbatch,
            retries: 0,
            envs: HashMap::new(),
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            tags: vec![],
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm_override: SlurmOverride::new(),
        }
    }

    fn get_ctx(nodes: &[Node]) -> Ctx {
        let mut ctx = Ctx::new(nodes).unwrap();
        ctx.try_nums[0] = 1;
        ctx
    }

    #[test]
    fn slurm_status_parsed_correctly() {
        let slurm_status = "
        JobID             State
        ------------ ----------
        20836188      COMPLETED
        20836188.ba+  COMPLETED
        20836188.ex+  COMPLETED
        ";
        let output = parse_slurm_status("20836188", slurm_status).unwrap();
        assert_eq!(output, "COMPLETED");
    }

    #[test]
    fn slurm_status_not_found() {
        let slurm_status = "
        JobID             State
        ------------ ----------
        20836188.ba+  COMPLETED
        20836188.ex+  COMPLETED
        ";
        let output = parse_slurm_status("20836188", slurm_status);
        assert!(output.is_none());
    }

    #[test]
    fn get_failed_slurm_status() {
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188      FAILED
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let output = parse_slurm_status("20836188", slurm_status).unwrap();
        assert_eq!(output, "FAILED");
    }

    #[test]
    fn running_slurm_status() {
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188      COMPLETING
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let output = parse_slurm_status("20836188", slurm_status).unwrap();
        assert_eq!(output, "COMPLETING");
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
        assert!(poll_slurm(&[]).unwrap().len() == 0)
    }

    #[test]
    fn test_mark_all_jobs_as_failed() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let task = get_task(0);
        let nodes = [Node::Task(task)];
        let mut ctx = get_ctx(&nodes);
        ctx.slurm_jobs.push((0, "111".into()));

        poller.mark_all_jobs_as_failed(&mut ctx);
        assert!(matches!(ctx.statuses[0], Status::Failed(_)));
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
        assert!(matches!(ctx.statuses[0], Status::Failed(_)))
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
        assert!(matches!(job, Job::Task(0)))
    }

    #[test]
    fn test_handle_completed_with_caching() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);

        let mut task = get_task(0);
        task.cache = true;

        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        poller.handle_new_slurm_status(
            &task,
            Status::Completed(Completed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );

        let job = ctx.jobs.pop_front().unwrap();
        assert!(matches!(job, Job::SaveCache(0)))
    }

    #[test]
    fn test_handle_completed_ext() {
        let cfg = get_cfg();
        let mut poller = get_poller(&cfg);
        let mut task = get_task(0);
        task.mode = ExecMode::Ext;

        let nodes = [Node::Task(task.clone())];
        let mut ctx = get_ctx(&nodes);
        poller.handle_new_slurm_status(
            &task,
            Status::Completed(Completed::Job(JobType::Slurm("123".into()))),
            &mut ctx,
        );

        let job = ctx.jobs.pop_front().unwrap();
        assert!(matches!(job, Job::SaveExtOutput(0, _)))
    }
}
