use crate::context::Ctx;
use crate::status::{Failed, JobType, Status};
use crate::workdirs;
use crate::{settings, state};
use log;
use std::io;
use std::process::{Command, Stdio};

pub fn create_kill_lock(name: &str, hash: &str) {
    log::info!("Killing pipeline '{name}' with hash '{hash}'");
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let Some(dagdir) = workdirs::find_pipeline_folder(&homedir, name, hash) else {
        log::warn!("No '{}' pipeline runs found for hash '{}'", name, hash);
        return;
    };

    log::debug!(
        "Pipeline {name}; Directory identified: '{}'",
        dagdir.to_string_lossy()
    );

    match state::create_kill_file(&dagdir) {
        Ok(_) => log::info!("Pipeline '{name}', hash '{hash}': Kill signal emitted"),
        Err(e) => log::error!(
            "Pipeline '{name}', hash '{hash}': Failed \
            to create kill file - {e}"
        ),
    }
}

pub fn kill_jobs(ctx: &mut Ctx) {
    if let Err(e) = kill_slurm_jobs(ctx) {
        log::error!("Failed to kill slurm jobs - {e}");
    };

    kill_local_jobs(ctx);
}

fn kill_local_jobs(ctx: &mut Ctx) {
    for (uid, child) in &mut ctx.local_jobs {
        let job_type = JobType::Local(child.id());
        ctx.statuses[*uid] = Status::Failed(Failed::Job(job_type));
        match child.kill() {
            Ok(_) => log::info!("Task '{uid}': Killed process {}", child.id()),
            Err(e) => log::error!("Task '{uid}': Failed to kill process {} - {e}", child.id()),
        }
    }
}

fn kill_slurm_jobs(ctx: &mut Ctx) -> io::Result<()> {
    let mut job_ids: Vec<&str> = Vec::with_capacity(ctx.slurm_jobs.len());
    for (uid, job_id) in &ctx.slurm_jobs {
        log::info!("Task '{}': Killing job {}", uid, job_id);
        let job_type = JobType::Slurm(job_id.to_string());
        ctx.statuses[*uid] = Status::Failed(Failed::Job(job_type));
        job_ids.push(job_id);
    }

    if job_ids.len() == 0 {
        log::warn!("No running jobs found");
        return Ok(());
    }

    let mut cmd = Command::new("scancel");
    for job in job_ids {
        cmd.arg(job);
    }

    cmd.stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};

    use super::*;
    #[test]
    fn test_job_kill() {
        let child = Command::new("sleep").arg("5").spawn().unwrap();
        let mut ctx = Ctx {
            updated: VecDeque::new(),
            statuses: vec![Status::Running(JobType::Local(child.id()))],
            jobs: VecDeque::new(),
            try_nums: vec![1],
            local_jobs: vec![(0, child)],
            slurm_jobs: Vec::new(),
            running_cacheable: HashSet::new(),
        };

        kill_jobs(&mut ctx);

        let exit_status = ctx.local_jobs[0].1.wait().unwrap();
        assert!(!exit_status.success())
    }
}
