//! Create the summary table out of the nodemap.

use crate::model::{Node, NodeBehavior};
use std::collections::HashMap;
use tabled::{
    Table, Tabled,
    settings::{Alignment, Style, object::Columns},
};

use crate::model::JobStatus;

/// Summary table.
#[derive(Tabled)]
struct Summary<'a> {
    /// Node unique id.
    uid: &'a str,
    /// Task name.
    task: &'a str,
    /// Node status.
    status: &'a JobStatus,
    /// Number of tries.
    num_tries: u32,
}

/// Build and return the summary table.
pub fn get_summary_table<'a>(nodemap: &'a HashMap<String, Node>) -> Table {
    let mut records = Vec::new();
    for node in nodemap.values() {
        if let NodeBehavior::TaskNode(task) = &node.behavior {
            records.push(Summary {
                uid: &node.uid,
                task: &task.name,
                status: &node.status,
                num_tries: task.try_num,
            });
        }
    }

    records.sort_by_key(|record| (record.uid.chars().count(), record.uid));
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

/// Print the recap table
///
/// It contains the number of tasks ended in each state
pub fn print_recap(nodemap: &HashMap<String, Node>) {
    let table = get_final_recap_table(nodemap);
    log::info!("Final recap:\n{}", table)
}

/// Get the recap table
fn get_final_recap_table(nodemap: &HashMap<String, Node>) -> Table {
    let mut ncompleted = 0;
    let mut nskipped = 0;
    let mut nfailed = 0;
    let mut nrunning = 0;
    let mut nnot_submitted = 0;

    for node in nodemap.values() {
        if !matches!(node.behavior, NodeBehavior::TaskNode(_)) {
            continue;
        }
        match node.status {
            JobStatus::Completed(_) => ncompleted += 1,
            JobStatus::Skipped => nskipped += 1,
            JobStatus::Failed(_) => {
                log::warn!("Task '{}' ended in a failure status", node.uid);
                nfailed += 1
            }
            JobStatus::Running(_) => {
                log::error!("Found running task '{}'", node.uid);
                nrunning += 1
            }
            JobStatus::NotSubmitted | JobStatus::ReadyForSubmission => {
                log::error!("Task '{}' has not been submitted", node.uid);
                nnot_submitted += 1
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
    use crate::model::{Cmd, ExecMode, NodeFailure, NodeResult, Task};
    use tabled::assert::assert_table;

    /// Check the summary table is correct.
    #[test]
    fn check_table() {
        let nodemap = HashMap::from([
            (
                String::from("2"),
                Node {
                    uid: String::from("2"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage1"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Failed(NodeFailure::Task("1234".to_string())),
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("10"),
                Node {
                    uid: String::from("10"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage2"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 2,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Skipped,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
        ]);
        let table = get_summary_table(&nodemap);
        assert_table!(table,
        "┌─────┬────────┬───────────────┬───────────┐"
        "│ uid │ task   │ status        │ num_tries │"
        "├─────┼────────┼───────────────┼───────────┤"
        "│   2 │ stage1 │ Failed (1234) │ 1         │"
        "├─────┼────────┼───────────────┼───────────┤"
        "│  10 │ stage2 │ Skipped       │ 2         │"
        "└─────┴────────┴───────────────┴───────────┘"
        );
    }

    /// Test the final recap table calculation
    #[test]
    fn get_recap_table() {
        let nodemap = HashMap::from([
            (
                String::from("0"),
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage1"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Failed(NodeFailure::Task("1234".to_string())),
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage1"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Completed(NodeResult::Node),
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("2"),
                Node {
                    uid: String::from("2"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage1"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Skipped,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("3"),
                Node {
                    uid: String::from("3"),
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("stage1"),
                        caching: false,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    }),
                    status: JobStatus::Running("5678".to_string()),
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
        ]);
        let table = get_final_recap_table(&nodemap);
        assert_table!(table,
            "┌───────────────┬────────┐"
            "│        status │ ntasks │"
            "├───────────────┼────────┤"
            "│     Completed │ 1      │"
            "├───────────────┼────────┤"
            "│       Skipped │ 1      │"
            "├───────────────┼────────┤"
            "│        Failed │ 1      │"
            "├───────────────┼────────┤"
            "│       Running │ 1      │"
            "├───────────────┼────────┤"
            "│ Not submitted │ 0      │"
            "└───────────────┴────────┘"
        );
    }
}
