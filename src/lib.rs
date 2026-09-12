use pyo3::prelude::*;
mod banner;
mod cache_pruning;
mod context;
mod dag_setup;
mod describe;
mod kill;
mod list_pipelines;
mod nodes;
mod polling;
mod run_task;
mod scheduler;
mod schemas;
mod settings;
mod state;
mod status;
mod submission;
mod summary;
mod visitors;
mod workdirs;

#[pymodule]
mod core {
    use super::*;

    #[pyfunction]
    #[pyo3(signature = (
        pipeline_path: "str",
        max_concurrency: "int",
        time_between_polls: "int",
        local: "bool",
        log_level: "str",
    ) -> "None")]
    pub fn run(
        pipeline_path: &str,
        max_concurrency: usize,
        time_between_polls: u64,
        local: bool,
        log_level: &str,
    ) {
        settings::configure_logging(log_level);
        scheduler::run(pipeline_path, max_concurrency, time_between_polls, local)
    }

    #[pyfunction]
    #[pyo3(signature =(
        pipeline_name: "str",
        pipeline_hash: "str",
        max_concurrency:"int",
        time_between_polls:"int",
        log_level: "str",
        retry: "bool",
    ) -> "None")]
    pub fn restart_run(
        pipeline_name: &str,
        pipeline_hash: &str,
        max_concurrency: usize,
        time_between_polls: u64,
        log_level: &str,
        retry: bool,
    ) {
        settings::configure_logging(log_level);
        scheduler::restart_run(
            pipeline_name,
            pipeline_hash,
            max_concurrency,
            time_between_polls,
            retry,
        );
    }

    #[pyfunction]
    #[pyo3(signature=(
        pipeline_name: "str",
        pipeline_hash: "str",
        log_level: "str",
    ) -> "None")]
    pub fn kill_run(pipeline_name: &str, pipeline_hash: &str, log_level: &str) {
        settings::configure_logging(log_level);
        kill::create_kill_lock(pipeline_name, pipeline_hash);
    }

    #[pyfunction]
    #[pyo3(signature=(
        task_name: "str",
        pipeline_name: "str | None",
        allow_full_prune: "bool",
        log_level: "str"
    ) -> "None")]
    pub fn prune_cache(
        task_name: &str,
        pipeline_name: Option<&str>,
        allow_full_prune: bool,
        log_level: &str,
    ) {
        settings::configure_logging(log_level);
        cache_pruning::prune_cache(task_name, pipeline_name, allow_full_prune);
    }

    #[pyfunction]
    #[pyo3(signature=(
        mermaid: "str",
        log_level: "str",
    ) -> "None")]
    pub fn view_pipeline(mermaid: &str, log_level: &str) {
        use graphs_tui::{RenderOptions, render_mermaid_to_tui};

        settings::configure_logging(log_level);
        log::debug!("\n{mermaid}");

        let result =
            render_mermaid_to_tui(mermaid, RenderOptions::default()).expect("Failed to print DAG");
        println!("{}", result.output);
    }

    #[pyfunction]
    #[pyo3(signature=(
        task_serialized: "str",
        log_level: "str",
    ) -> "None")]
    pub fn run_single_task(task_serialized: &str, log_level: &str) {
        settings::configure_logging(log_level);
        run_task::run_task(task_serialized);
    }

    #[pyfunction]
    #[pyo3(signature=(
        names: "list[str]",
        paths: "list[str]",
    ) -> "None")]
    pub fn print_pipeline_list(names: Vec<String>, paths: Vec<String>) {
        list_pipelines::print_pipelines(&names, &paths);
    }

    #[pyfunction]
    #[pyo3(signature=(
        pipeline_name: "str",
        log_level: "str",
    ) -> "None")]
    pub fn print_runs(pipeline_name: &str, log_level: &str) {
        settings::configure_logging(log_level);
        if let Err(e) = list_pipelines::print_runs(pipeline_name) {
            log::error!("Failed to retrieve data - {e}");
        }
    }

    #[pyfunction]
    #[pyo3(signature = (
        pipeline_path: "str",
        log_level: "str",
    ) -> "None")]
    pub fn describe_pipeline(pipeline_path: &str, log_level: &str) {
        settings::configure_logging(log_level);
        describe::describe_pipeline(pipeline_path)
    }
}
