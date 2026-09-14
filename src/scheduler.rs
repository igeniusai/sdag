use crate::context::Ctx;
use crate::nodes::Node;
use crate::polling::{LocalPoller, Poller, SlurmPoller};
use crate::schemas::DAGMeta;
use crate::settings;
use crate::settings::Cfg;
use crate::state;
use crate::submission::Submitter;
use crate::summary;
use crate::visitors::NodeVisitor;
use crate::workdirs;
use crate::{banner, status};
use crate::{dag_setup, kill};
use ctrlc;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::{Mutex, Once};
use std::time::Duration;

pub fn run(
    pipeline_path: &str,
    max_concurrency: usize,
    time_between_polls: u64,
    slurm_grace_period: usize,
    max_concurrent_runs: usize,
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
    let cfg = Cfg::new(
        &homedir,
        &meta,
        max_concurrency,
        time_between_polls,
        slurm_grace_period,
        max_concurrent_runs,
    );

    dag_setup::add_children(&mut nodes);
    workdirs::create_dir_structure(&cfg, &nodes).expect("Failed to create dagdir");
    if let Err(e) = state::copy_dag_in_dagdir(&pipeline_path, &cfg.dagdir) {
        log::error!("Failed to copy DAG into the DAG directory - {e}");
    }

    let mut ctx = Ctx::new(&nodes).unwrap();
    scheduling_loop(&mut nodes, &mut ctx, &cfg, &meta);
}

// TODO: Validate directory structure
pub fn restart_run(
    name: &str,
    hash: &str,
    max_concurrency: usize,
    time_between_polls: u64,
    retry: bool,
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

    scheduling_loop(&nodes, &mut ctx, &cfg, &meta);
}

static CTRLC_TX: Mutex<Option<Sender<()>>> = Mutex::new(None);
static ONCE: Once = Once::new();

fn install_ctrlc_handler() {
    ONCE.call_once(|| {
        ctrlc::set_handler(|| {
            if let Ok(guard) = CTRLC_TX.lock() {
                if let Some(tx) = guard.as_ref() {
                    let _ = tx.send(());
                }
            }
        })
        .expect("Error setting Ctrl-C handler");
    });
}

fn scheduling_loop(nodes: &[Node], ctx: &mut Ctx, cfg: &Cfg, meta: &DAGMeta) {
    banner::log_banner();
    log::info!(
        "Start scheduling pipeline '{}' with hash '{}'",
        meta.pipeline_name,
        meta.hash
    );
    state::rm_kill_file_if_present(&cfg.dagdir);

    let mut visitor = NodeVisitor { nodes };
    let mut submitter = Submitter::new(&cfg, &meta);
    let mut slurm_poller = SlurmPoller::new(&cfg);
    let mut local_poller = LocalPoller;

    let root_uid = dag_setup::find_root_node(nodes).expect("Failed to find the root node");
    ctx.updated.push_back(root_uid);

    let (tx, rx) = mpsc::channel();
    install_ctrlc_handler();
    *CTRLC_TX.lock().expect("Ctrl-C mutex poisoned") = Some(tx);

    while !state::is_scheduler_killed(&cfg.dagdir) {
        slurm_poller.poll(&nodes, ctx);
        local_poller.poll(&nodes, ctx);
        visitor.visit(ctx);
        submitter.submit(&nodes, ctx);

        if let Err(e) = state::save_checkpoint(&cfg, meta, &nodes, ctx) {
            log::error!("Failed to save checkpoint: {e}");
        };

        summary::print_summary(&nodes, &ctx, &meta.pipeline_name, &meta.hash);
        if status::is_simulation_completed(&ctx.statuses) {
            log::info!("Simulation completed");
            break;
        }

        if let Ok(_) = rx.recv_timeout(cfg.sleep_time) {
            print!("\nScheduler stopped, do you wanna kill all running jobs? (y/N) ");
            let choice: String = text_io::read!("{}");
            if choice == "y" {
                let _ = state::create_kill_file(&cfg.dagdir)
                    .map_err(|e| log::error!("Failed to create kill file - {e}"));
            }
            break;
        }
    }

    if state::is_scheduler_killed(&cfg.dagdir) {
        log::info!("Killing all jobs");
        kill::kill_jobs(ctx);
    }

    let _ = state::save_checkpoint(&cfg, meta, &nodes, ctx);
    summary::print_recap(&nodes, ctx);

    *CTRLC_TX.lock().expect("Ctrl-C mutex poisoned") = None;
}
