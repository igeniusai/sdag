use crate::context::Ctx;
use crate::nodes::Node;
use crate::settings::Cfg;
use crate::state;
use crate::status::Status;
use log;
use std::path::Path;

use crate::settings;
use crate::workdirs;

pub fn create_skip_lock(name: &str, hash: &str) {
    log::info!("Skipping breakpoint of pipeline '{name}' with hash '{hash}'");
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let Some(dagdir) = workdirs::find_pipeline_folder(&homedir, name, hash) else {
        log::warn!("No '{}' pipeline runs found for hash '{}'", name, hash);
        return;
    };

    log::debug!(
        "Pipeline {name}; Directory identified: '{}'",
        dagdir.to_string_lossy()
    );

    match state::create_skip_file(&dagdir) {
        Ok(_) => log::info!("Pipeline '{name}', hash '{hash}': Skip signal emitted"),
        Err(e) => log::error!(
            "Pipeline '{name}', hash '{hash}': Failed \
            to create skip file - {e}"
        ),
    }
}

pub fn create_continue_lock(name: &str, hash: &str) {
    log::info!("Continuing breakpoint of pipeline '{name}' with hash '{hash}'");
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let Some(dagdir) = workdirs::find_pipeline_folder(&homedir, name, hash) else {
        log::warn!("No '{}' pipeline runs found for hash '{}'", name, hash);
        return;
    };

    log::debug!(
        "Pipeline {name}; Directory identified: '{}'",
        dagdir.to_string_lossy()
    );

    match state::create_continue_file(&dagdir) {
        Ok(_) => log::info!("Pipeline '{name}', hash '{hash}': Skip signal emitted"),
        Err(e) => log::error!(
            "Pipeline '{name}', hash '{hash}': Failed \
            to create continue file - {e}"
        ),
    }
}

pub struct Debugger<'a> {
    pub monitored: Vec<usize>,
    pub pipeline_name: &'a str,
    pub hash: &'a str,
}

impl<'a> Debugger<'a> {
    pub fn new(nodes: &[Node], pipeline_name: &'a str, hash: &'a str) -> Self {
        let mut monitored = vec![];
        for node in nodes {
            if let Node::Task(task) = node
                && task.debug
            {
                monitored.push(task.uid);
            }
        }

        Debugger {
            monitored,
            pipeline_name,
            hash,
        }
    }

    pub fn break_if_failed(&mut self, cfg: &Cfg, ctx: &mut Ctx) {
        if self.any_monitored_task_failed(&ctx.statuses) {
            log::warn!(
                "Scheduler interrupted. In a new terminal run:\n\
                - 'sdag continue {0} --hash {1}': continue \
                from this checkpoint after fixing the issue.\n\
                - 'sdag skip {0} --hash {1}: skip this breakpoint \
                and stop tracking the failed tasks.\n\
                - 'sdag kill {0} --hash {1}': kill the pipeline.",
                self.pipeline_name,
                self.hash
            );
            self.remove_locks(&cfg.dagdir);
            while !state::is_scheduler_killed(&cfg.dagdir) {
                if state::is_breakpoint_continued(&cfg.dagdir) {
                    log::info!("Continuing from breakpoint");
                    self.remove_locks(&cfg.dagdir);
                    self.update_failed_tasks(ctx);
                    break;
                }
                if state::is_breakpoint_skipped(&cfg.dagdir) {
                    log::info!("Breakpoint skipped");
                    self.remove_locks(&cfg.dagdir);
                    self.ignore_breakpoints(&ctx.statuses);
                    break;
                }
            }
        }
    }

    fn any_monitored_task_failed(&self, statuses: &[Status]) -> bool {
        for uid in &self.monitored {
            if matches!(statuses[*uid], Status::Failed(_)) {
                log::warn!("Task {uid} failed, breakpointing...");
                return true;
            }
        }
        false
    }

    fn update_failed_tasks(&self, ctx: &mut Ctx) {
        for uid in &self.monitored {
            if matches!(ctx.statuses[*uid], Status::Failed(_)) {
                log::info!("Resetting task {uid}");
                ctx.statuses[*uid] = Status::ReadyForSubmission;
                ctx.try_nums[*uid] = 0;
                ctx.updated.push_back(*uid);
            }
        }
    }

    fn ignore_breakpoints(&mut self, statuses: &[Status]) {
        self.monitored = self
            .monitored
            .iter()
            .filter(|uid| !matches!(statuses[**uid], Status::Failed(_)))
            .map(|uid| *uid)
            .collect();
    }

    fn remove_locks(&self, dagdir: &Path) {
        state::rm_continue_lock_if_present(&dagdir);
        state::rm_skip_lock_if_present(&dagdir);
    }
}
