use crate::engine::context::Ctx;
use crate::engine::polling::Poller;
use crate::engine::submission::{self, inline};
use crate::model::nodes::{Node, Task};
use crate::model::schemas::ExecMode;
use crate::model::status::JobType::Slurm;
use crate::model::status::{Completed, Failed, JobType, Status};
use crate::settings::Cfg;
use log;
use regex::Regex;
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

        let mut poll_result = match poll_slurm(&ctx.slurm_jobs) {
            Ok(poll_result) => poll_result,
            Err(e) => {
                log::error!("Failed to contact Slurm - {e}");
                HashMap::new()
            }
        };

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
            Status::Pending(JobType::Slurm(job_id)) | Status::Running(JobType::Slurm(job_id)) => {
                ctx.slurm_jobs.push((task.uid, job_id.to_string()));
                ctx.statuses[task.uid] = status;
            }
            Status::Failed(failure) => {
                log::error!("Task - '{}' failed", task.uid);
                submission::handle_retries(task, ctx, failure.clone())
            }
            Status::Completed(Completed::Job(job_type)) => {
                ctx.running_cacheable.remove(&task.name);
                ctx.updated.push_back(task.uid);
                if let ExecMode::Ext = task.mode
                    && let Err(e) = inline::submit_save_ext_output(task, &self.cfg)
                {
                    log::error!("Task {}: Failed to save external output - {e}", task.uid);
                    ctx.statuses[task.uid] = Status::Failed(Failed::Job(job_type.clone()));
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
                ctx.statuses[task.uid] = status;
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
    use crate::engine::context::Job;
    use crate::store::workdirs::FileNames;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    use super::*;
    use crate::model::schemas::{Cmd, DAGMeta, Scope, Script, ScriptPath, SlurmOverride};
    use serde_json::Value;
    use std::env;
    use uuid::Uuid;

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

    fn get_tmp_dir() -> PathBuf {
        let path = env::temp_dir().join(Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn get_cfg() -> Cfg {
        let meta = get_meta();
        let homedir = get_tmp_dir();
        Cfg::new(&homedir, &meta, "info", 1, 5, 1, 1, false)
    }

    fn get_poller(cfg: &Cfg) -> SlurmPoller<'_> {
        SlurmPoller {
            cfg,
            missing_jobs: HashMap::new(),
        }
    }

    fn get_task(uid: usize) -> Task {
        Task {
            uid,
            parents: vec![],
            scope: Scope::Local,
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: false,
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
            slurm: SlurmOverride::default(),
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
    }
}
