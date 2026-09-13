use crate::context::{Ctx, Job};
use crate::nodes::{Node, Task};
use crate::polling::Poller;
use crate::schemas::ExecMode;
use crate::status::{Completed, Failed, JobType, Status};
use crate::submission;
use std::process::Child;

pub struct LocalPoller;
impl Poller for LocalPoller {
    fn poll(&mut self, nodes: &[Node], ctx: &mut Ctx) {
        let mut local_jobs = Vec::new();
        while let Some((uid, child)) = ctx.local_jobs.pop() {
            if let Node::Task(task) = &nodes[uid] {
                self.check_status(child, task, ctx, &mut local_jobs);
            }
        }
        ctx.local_jobs = local_jobs;
    }
}

impl LocalPoller {
    fn check_status(
        &mut self,
        mut child: Child,
        task: &Task,
        ctx: &mut Ctx,
        local_jobs: &mut Vec<(usize, Child)>,
    ) {
        let uid = task.uid;
        let status = poll_local(&mut child);
        match status {
            Status::Running(_) => {
                log::info!("Local task '{uid}' is running");
                local_jobs.push((uid, child))
            }
            Status::Completed(_) => {
                log::info!("Local task '{uid}' completed");
                ctx.statuses[task.uid] = status;
                self.handle_success(task, child.id(), ctx);
            }
            _ => {
                log::error!("Local task '{uid}' failed");
                let failure = Failed::Job(JobType::Local(child.id()));
                submission::handle_retries(task, ctx, failure);
            }
        }
    }

    fn handle_success(&self, task: &Task, pid: u32, ctx: &mut Ctx) {
        if let ExecMode::Ext = task.mode {
            log::debug!("Task '{}': Queuing output save", task.uid);
            let job_type = JobType::Local(pid);
            ctx.jobs.push_back(Job::SaveExtOutput(task.uid, job_type));
            return;
        }
        ctx.running_cacheable.remove(&task.name);
        ctx.updated.push_back(task.uid);
        if task.cache | task.cache_local {
            log::debug!("task '{}': Queueing cache save", task.uid);
            ctx.jobs.push_back(Job::SaveCache(task.uid));
        }
    }
}

fn poll_local(child: &mut Child) -> Status {
    let pid = child.id();
    let job_type = JobType::Local(pid);
    match child.try_wait() {
        Ok(Some(status)) => {
            if status.success() {
                log::info!("Local task with pid {} succeeded.", pid);
                Status::Completed(Completed::Job(job_type))
            } else {
                log::error!("Local task with pid {} failed.", pid);
                Status::Failed(Failed::Job(job_type))
            }
        }
        Ok(None) => {
            log::debug!("Local task with pid {} is running.", pid);
            Status::Running(job_type)
        }
        Err(e) => {
            log::error!("Failed to fetch status for local process {} - {e}", pid);
            Status::Failed(Failed::Job(job_type))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schemas::{Cmd, Script, ScriptPath, SlurmOverride};
    use std::collections::HashMap;
    use std::process::Command;

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
            cmd: Cmd::Bash,
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

    #[test]
    fn test_success_poll() {
        let mut child = Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let status = poll_local(&mut child);
        assert!(matches!(status, Status::Completed(Completed::Job(_))));
    }

    #[test]
    fn test_poll() {
        let nodes = vec![Node::Task(get_task(0))];
        let mut ctx = Ctx::new(&nodes).unwrap();

        let mut child = Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        ctx.local_jobs.push((0, child));

        let mut poller = LocalPoller;
        poller.poll(&nodes, &mut ctx);

        assert!(matches!(
            ctx.statuses[0],
            Status::Completed(Completed::Job(_))
        ))
    }
}
