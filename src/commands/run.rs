// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use crate::engine::{context::Ctx, scheduler};
use crate::model::{dag_setup, status};
use crate::settings::{self, Cfg};
use crate::store::state;
use crate::store::workdirs::{self, DirPaths};
use std::path::PathBuf;
use std::time::Duration;

pub fn run(
    pipeline_path: &str,
    max_concurrency: usize,
    time_between_polls: u64,
    slurm_grace_period: usize,
    max_concurrent_runs: usize,
    log_level: &str,
    fail_fast: bool,
) {
    let pipeline_path = PathBuf::from(pipeline_path);
    let dag = state::read_dag(&pipeline_path).expect("Failed to read DAG - {e}");
    log::info!(
        "Running pipeline '{}' with hash '{}'",
        dag.meta.pipeline_name,
        dag.meta.hash
    );

    let (meta, mut nodes) = (dag.meta, dag.nodes);
    nodes.sort_by_key(|n| n.get_uid());

    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let paths = DirPaths::new(&homedir, &meta.pipeline_name, &meta.hash);
    let cfg = Cfg {
        homedir,
        dagdir: paths.dagdir,
        cachedir: paths.cachedir,
        local_cachedir: paths.local_cachedir,
        timestamp: settings::get_timestamp(),
        grace_period: slurm_grace_period,
        max_dagdirs: max_concurrent_runs,
        max_concurrency,
        sleep_time: Duration::from_secs(time_between_polls),
        log_level: log_level.to_string(),
        fail_fast,
    };

    dag_setup::add_children(&mut nodes);
    workdirs::create_dir_structure(&cfg, &nodes).expect("Failed to create dagdir");
    if let Err(e) = state::copy_dag_in_dagdir(&pipeline_path, &cfg.dagdir) {
        log::error!("Failed to copy DAG into the DAG directory - {e}");
    }

    let mut ctx = Ctx::new(&nodes).unwrap();
    scheduler::scheduling_loop(&mut nodes, &mut ctx, &cfg, &meta);
}

pub fn restart_run(
    name: &str,
    hash: &str,
    max_concurrency: usize,
    time_between_polls: u64,
    retry: bool,
    log_level: &str,
    fail_fast: bool,
) {
    let homedir = settings::find_homedir().expect("Failed to find the home directory");
    let Some(path) = workdirs::find_pipeline_folder(&homedir, name, hash) else {
        log::warn!("No '{}' pipeline runs found for hash '{}'", name, hash);
        return;
    };

    let ckpt = state::read_chekpoint(&path).expect("Failed to read checkpoint");
    let meta = ckpt.meta.into_owned();
    let nodes = ckpt.nodes.into_owned();
    let mut try_nums = ckpt.try_nums.into_owned();
    let mut statuses = ckpt.statuses.into_owned();
    if retry {
        log::info!("Retry enabled by user");
        status::reset_failed_or_skipped_status(&mut statuses, &mut try_nums);
    }

    let mut ctx = Ctx::from_checkpoint(&nodes, statuses, try_nums);
    let mut cfg = ckpt.cfg.into_owned();
    cfg.max_concurrency = max_concurrency;
    cfg.sleep_time = Duration::from_secs(time_between_polls);
    cfg.log_level = log_level.to_string();
    cfg.fail_fast = fail_fast;

    scheduler::scheduling_loop(&nodes, &mut ctx, &cfg, &meta);
}
