pub mod blocking;
pub mod nonblocking;
use crate::context::{Ctx, Job};
use crate::nodes::{Branch, Node, OneOf, Task};
use crate::schemas::{Cmd, DAGMeta};
use crate::settings::Cfg;
use crate::status::{Completed, Failed, JobType, Status};
use std::error::Error;

use std::collections::VecDeque;

pub fn handle_retries(task: &Task, ctx: &mut Ctx, failure: Failed) {
    let try_num = &ctx.try_nums[task.uid];
    if *try_num > task.retries {
        log::error!(
            "Task '{}': Maximum number of retries '{}' reached, marking as failed.",
            task.uid,
            task.retries
        );
        ctx.statuses[task.uid] = Status::Failed(failure);
        ctx.updated.push_back(task.uid);
    } else {
        log::warn!(
            "Task '{}' failed, scheduling retry '{}/{}'.",
            task.uid,
            try_num,
            task.retries,
        );
        ctx.jobs.push_back(Job::Task(task.uid));
    }
}

pub struct Submitter<'a> {
    pub cfg: &'a Cfg,
    pub meta: &'a DAGMeta,
    pub held_jobs: VecDeque<Job>,
}

impl<'a> Submitter<'a> {
    pub fn new(cfg: &'a Cfg, meta: &'a DAGMeta) -> Self {
        Self {
            cfg,
            meta,
            held_jobs: VecDeque::new(),
        }
    }

    pub fn submit(&mut self, nodes: &[Node], ctx: &mut Ctx) {
        while let Some(job) = self.held_jobs.pop_front() {
            ctx.jobs.push_front(job);
        }

        while let Some(job) = ctx.jobs.pop_front() {
            match job {
                Job::Task(uid) => {
                    if let Node::Task(task) = &nodes[uid] {
                        self.submit_task(task, ctx)
                    }
                }
                Job::Branch(uid) => {
                    if let Node::Branch(branch) = &nodes[uid] {
                        self.submit_branch(branch, ctx);
                    }
                }
                Job::OneOf(uid, target) => {
                    if let Node::OneOf(oneof) = &nodes[uid] {
                        self.submit_oneof(oneof, target, ctx);
                    }
                }
                Job::SaveCache(uid) => {
                    if let Node::Task(task) = &nodes[uid] {
                        self.submit_save_cache(task, ctx);
                    }
                }
                Job::ValidateCache(uid) => {
                    if let Node::Task(task) = &nodes[uid] {
                        self.submit_validate_cache(task, ctx);
                    }
                }
                Job::SaveExtOutput(uid, job_type) => {
                    if let Node::Task(task) = &nodes[uid] {
                        self.submit_save_ext_output(task, job_type, ctx);
                    }
                }
            }
        }
    }

    fn submit_branch(&mut self, branch: &Branch, ctx: &mut Ctx) {
        ctx.statuses[branch.uid] = match blocking::submit_branch(branch, &self.cfg) {
            Ok(choice) => Status::Completed(Completed::Branch(choice)),
            Err(_) => Status::Failed(Failed::Generic),
        };
        ctx.updated.push_back(branch.uid);
    }

    fn submit_task(&mut self, task: &Task, ctx: &mut Ctx) {
        let nrunning = ctx.nrunning();
        if self.cfg.max_concurrency != 0 && nrunning >= self.cfg.max_concurrency {
            log::info!(
                "Holding back task '{}' as the number of running \
                jobs '{}' has reached the max concurrency '{}'",
                task.uid,
                nrunning,
                self.cfg.max_concurrency
            );
            self.held_jobs.push_back(Job::Task(task.uid));
            return;
        }

        ctx.try_nums[task.uid] += 1;
        let res = match &task.cmd {
            Cmd::Sbatch => self.submit_slurm(task, ctx),
            Cmd::Bash => self.submit_local(task, ctx),
        };

        if let Err(e) = res {
            log::error!("Task '{}': submission failed - {e}", task.uid);
            let failure = Failed::Generic;
            handle_retries(task, ctx, failure);
        }

        if task.cache && ctx.statuses[task.uid].is_running() {
            log::debug!("Task {} marked as running cacheable", task.uid);
            ctx.running_cacheable.insert(task.name.to_string());
        }
    }

    fn submit_local(&mut self, task: &Task, ctx: &mut Ctx) -> Result<(), Box<dyn Error>> {
        let try_num = ctx.try_nums[task.uid];
        let child = nonblocking::submit_local(task, try_num, &self.cfg, &self.meta)?;
        let pid = child.id();

        log::info!("Task '{}': Submitted process with pid '{}'", task.uid, pid);
        ctx.local_jobs.push((task.uid, child));
        ctx.statuses[task.uid] = Status::Running(JobType::Local(pid));

        Ok(())
    }

    fn submit_slurm(&mut self, task: &Task, ctx: &mut Ctx) -> Result<(), Box<dyn Error>> {
        let try_num = ctx.try_nums[task.uid];
        let job_id = nonblocking::submit_slurm(task, try_num, &self.cfg, &self.meta)?;

        log::info!("Task '{}': Submitted job_id '{}'", task.uid, job_id);
        ctx.slurm_jobs.push((task.uid, job_id.to_string()));
        ctx.statuses[task.uid] = Status::Pending(JobType::Slurm(job_id));

        Ok(())
    }

    fn submit_oneof(&mut self, oneof: &OneOf, target: usize, ctx: &mut Ctx) {
        match blocking::submit_oneof(oneof, target, &self.cfg) {
            Ok(_) => {
                log::info!("OneOf '{}': Submission succeeded", oneof.uid);
                ctx.statuses[oneof.uid] = Status::Completed(Completed::OneOf(target));
            }
            Err(e) => {
                log::info!("OneOf '{}': Submission failed - {e}", oneof.uid);
                ctx.statuses[oneof.uid] = Status::Failed(Failed::Generic);
            }
        }
        ctx.updated.push_back(oneof.uid);
    }

    fn submit_save_cache(&mut self, task: &Task, ctx: &mut Ctx) {
        match blocking::submit_save_cache(&task, &self.cfg) {
            Ok(_) => log::info!("Task '{}': Cache saved", task.uid),
            Err(e) => log::error!("Task '{}': Failed to save cache - {e}", task.uid),
        }
        ctx.running_cacheable.remove(&task.name);
        ctx.updated.push_back(task.uid);
    }

    fn submit_validate_cache(&mut self, task: &Task, ctx: &mut Ctx) {
        log::info!("checking task {} cache", task.uid);
        if task.cache && ctx.running_cacheable.contains(&task.name) {
            log::warn!(
                "Holding back task '{}' with name '{}': Another \
                cacheable task with the same name is already running",
                task.uid,
                task.name
            );
            self.held_jobs.push_back(Job::ValidateCache(task.uid));
            return;
        }

        let status = if blocking::submit_validate_cache(&task, &self.cfg) {
            log::info!("Task {} is cached.", task.uid);
            ctx.jobs.push_front(Job::SaveCache(task.uid));
            Status::Completed(Completed::Cached)
        } else {
            log::info!("Task {}: Cache validation failed.", task.uid);
            Status::ReadyForSubmission
        };
        ctx.statuses[task.uid] = status;
        ctx.updated.push_back(task.uid);
    }

    fn submit_save_ext_output(&mut self, task: &Task, job_type: JobType, ctx: &mut Ctx) {
        let res = blocking::submit_save_ext_output(task, &self.cfg);
        let status = match res {
            Ok(_) => {
                if task.cache || task.cache_local {
                    ctx.jobs.push_back(Job::SaveCache(task.uid));
                }
                log::info!("External task {} completed.", task.uid);
                Status::Completed(Completed::Job(job_type))
            }
            Err(_) => {
                log::error!("Task {}: Failed to save external output", task.uid);
                Status::Failed(Failed::Job(job_type))
            }
        };

        ctx.statuses[task.uid] = status;
        ctx.updated.push_back(task.uid);
    }
}
