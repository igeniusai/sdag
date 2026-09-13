//! Create the summary table out of the nodemap.

use crate::context::Ctx;
use crate::nodes::Node;
use tabled::{
    Table, Tabled,
    settings::{Alignment, Style, object::Columns},
};

use crate::status::Status;

/// Print the recap table
///
/// It contains the number of tasks ended in each state
pub fn print_recap(nodes: &[Node], ctx: &Ctx) {
    let table = get_final_recap_table(nodes, ctx);
    log::info!("Final recap:\n{}", table)
}

/// Print the summary table
pub fn print_summary(nodes: &[Node], ctx: &Ctx, pipeline_name: &str, hash: &str) {
    let table = get_summary_table(nodes, ctx);
    log::info!(
        "Pipeline '{}' with hash '{}' - Summary:\n{}",
        pipeline_name,
        hash,
        table
    )
}

/// Summary table.
#[derive(Tabled)]
struct Summary<'a> {
    /// Node unique id.
    uid: usize,
    /// Task name.
    task: &'a str,
    /// Pipeline
    pipeline: &'a str,
    /// Node status.
    status: &'a Status,
    /// Number of tries.
    try_num: usize,
}

/// Build and return the summary table.
fn get_summary_table<'a>(nodes: &[Node], ctx: &Ctx) -> Table {
    let mut records = Vec::new();
    for node in nodes {
        if let Node::Task(task) = &node {
            let try_num = ctx.try_nums[task.uid];
            let status = &ctx.statuses[task.uid];

            records.push(Summary {
                uid: task.uid,
                task: &task.name,
                pipeline: &task.pipeline_name,
                status,
                try_num,
            });
        }
    }

    records.sort_by_key(|record| (record.status.log_priority(), record.uid));

    let mut table = Table::new(records);
    table.with(Style::modern());
    table.modify(Columns::first(), Alignment::right());
    table
}

/// Summary table.
#[derive(Tabled)]
struct FinalRecap<'a> {
    /// Task status.
    status: &'a str,
    /// Number of tasks ended in status.
    ntasks: &'a u32,
}

/// Get the recap table
fn get_final_recap_table(nodes: &[Node], ctx: &Ctx) -> Table {
    let mut ncompleted = 0;
    let mut nskipped = 0;
    let mut nfailed = 0;
    let mut npending = 0;
    let mut nrunning = 0;
    let mut nnot_submitted = 0;

    for node in nodes {
        if let Node::Task(task) = &node {
            let status = &ctx.statuses[task.uid];
            match &status {
                Status::Completed(_) => ncompleted += 1,
                Status::Skipped => nskipped += 1,
                Status::Failed(_) => {
                    log::warn!("Task '{}' failed", task.uid);
                    nfailed += 1
                }
                Status::Pending(_) => {
                    log::error!("Found running task '{}'", task.uid);
                    npending += 1
                }
                Status::Running(_) => {
                    log::warn!("Found running task '{}'", task.uid);
                    nrunning += 1
                }
                Status::NotSubmitted | Status::ReadyForSubmission => {
                    log::warn!("Task '{}' has not been submitted", task.uid);
                    nnot_submitted += 1
                }
            }
        }
    }

    let records = vec![
        FinalRecap {
            status: "Completed",
            ntasks: &ncompleted,
        },
        FinalRecap {
            status: "Skipped",
            ntasks: &nskipped,
        },
        FinalRecap {
            status: "Failed",
            ntasks: &nfailed,
        },
        FinalRecap {
            status: "Pending",
            ntasks: &npending,
        },
        FinalRecap {
            status: "Running",
            ntasks: &nrunning,
        },
        FinalRecap {
            status: "Not submitted",
            ntasks: &nnot_submitted,
        },
    ];

    let mut table = Table::new(records);
    table.with(Style::modern());
    table.modify(Columns::first(), Alignment::right());
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::Task;
    use crate::schemas::{Cmd, ExecMode, Scope, Script, ScriptPath, SlurmOverride};
    use crate::status::{Completed, Failed, JobType};
    use std::collections::HashMap;
    use tabled::assert::assert_table;

    fn get_nodes() -> Vec<Node> {
        let task0 = Task {
            uid: 0,
            parents: vec![],
            fn_name: "fn_name".into(),
            name: "name".into(),
            scope: Scope::Local,
            pipeline_name: "pipeline_name".into(),
            cache: true,
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
        };

        let mut task1 = task0.clone();
        let mut task2 = task0.clone();
        let mut task3 = task0.clone();
        let mut task4 = task0.clone();

        task2.uid = 2;
        task1.uid = 1;
        task3.uid = 3;
        task4.uid = 4;

        vec![
            Node::Task(task0),
            Node::Task(task1),
            Node::Task(task2),
            Node::Task(task3),
            Node::Task(task4),
        ]
    }

    fn get_ctx(nodes: &[Node]) -> Ctx {
        let mut ctx = Ctx::new(nodes).unwrap();
        ctx.try_nums = vec![0, 1, 2, 3, 4, 5];
        ctx.statuses = vec![
            Status::Completed(Completed::Job(JobType::Slurm("123".into()))),
            Status::Failed(Failed::Generic),
            Status::Pending(JobType::Slurm("234".into())),
            Status::NotSubmitted,
            Status::Running(JobType::Slurm("456".into())),
        ];
        ctx
    }

    #[test]
    fn test_get_summary_table() {
        let nodes = get_nodes();
        let ctx = get_ctx(&nodes);
        let table = get_summary_table(&nodes, &ctx);
        assert_table!(table,
        "┌─────┬──────┬───────────────┬────────────────────────┬─────────┐"
        "│ uid │ task │ pipeline      │ status                 │ try_num │"
        "├─────┼──────┼───────────────┼────────────────────────┼─────────┤"
        "│   3 │ name │ pipeline_name │ Not Submitted          │ 3       │"
        "├─────┼──────┼───────────────┼────────────────────────┼─────────┤"
        "│   0 │ name │ pipeline_name │ Completed (job_id=123) │ 0       │"
        "├─────┼──────┼───────────────┼────────────────────────┼─────────┤"
        "│   1 │ name │ pipeline_name │ Failed                 │ 1       │"
        "├─────┼──────┼───────────────┼────────────────────────┼─────────┤"
        "│   2 │ name │ pipeline_name │ Pending (job_id=234)   │ 2       │"
        "├─────┼──────┼───────────────┼────────────────────────┼─────────┤"
        "│   4 │ name │ pipeline_name │ Running (job_id=456)   │ 4       │"
        "└─────┴──────┴───────────────┴────────────────────────┴─────────┘"
        );
    }

    #[test]
    fn test_get_final_recap_table() {
        let nodes = get_nodes();
        let ctx = get_ctx(&nodes);
        let table = get_final_recap_table(&nodes, &ctx);
        assert_table!(table,
        "┌───────────────┬────────┐"
        "│        status │ ntasks │"
        "├───────────────┼────────┤"
        "│     Completed │ 1      │"
        "├───────────────┼────────┤"
        "│       Skipped │ 0      │"
        "├───────────────┼────────┤"
        "│        Failed │ 1      │"
        "├───────────────┼────────┤"
        "│       Pending │ 1      │"
        "├───────────────┼────────┤"
        "│       Running │ 1      │"
        "├───────────────┼────────┤"
        "│ Not submitted │ 1      │"
        "└───────────────┴────────┘"
        );
    }
}
