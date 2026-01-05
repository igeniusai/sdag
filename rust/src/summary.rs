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
        if let NodeBehavior::TaskNode { fname, try_num, .. } = &node.behavior {
            records.push(Summary {
                uid: &node.uid,
                task: fname,
                status: &node.status,
                num_tries: *try_num,
            });
        }
    }

    records.sort_by_key(|record| (record.uid.chars().count(), record.uid));
    let mut table = Table::new(records);
    table.with(Style::modern());
    table.modify(Columns::first(), Alignment::right());
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeFailure;
    use tabled::assert::assert_table;

    /// Check the summary table is correct.
    #[test]
    fn check_table() {
        let nodemap = HashMap::from([
            (
                String::from("2"),
                Node {
                    uid: String::from("2"),
                    output_used: false,
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode {
                        fname: String::from("stage1"),
                        caching: false,
                        try_num: 1,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    },
                    status: JobStatus::Failed(NodeFailure::Task("1234".to_string())),
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("10"),
                Node {
                    uid: String::from("10"),
                    output_used: false,
                    output_artifacts: Vec::new(),
                    behavior: NodeBehavior::TaskNode {
                        fname: String::from("stage2"),
                        caching: false,
                        try_num: 2,
                        retries: 2,
                        launch_script: String::from("script"),
                        input_kwargs: Vec::new(),
                    },
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
}
