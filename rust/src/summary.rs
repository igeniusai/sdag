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
    /// Stage function name.
    stage: &'a str,
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
                stage: fname,
                status: &node.status,
                num_tries: *try_num,
            });
        }
    }

    records.sort_by_key(|record| record.uid);
    let mut table = Table::new(records);
    table.with(Style::modern());
    table.modify(Columns::first(), Alignment::right());
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use tabled::assert::assert_table;

    /// Check the summary table is correct.
    #[test]
    fn check_table() {
        let nodemap = HashMap::from([
            (
                String::from("n1"),
                Node {
                    uid: String::from("n1"),
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
                    status: JobStatus::Failed,
                    parents: Vec::new(),
                    children: Vec::new(),
                },
            ),
            (
                String::from("n2"),
                Node {
                    uid: String::from("n2"),
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
        "┌─────┬────────┬─────────┬───────────┐"
        "│ uid │ stage  │ status  │ num_tries │"
        "├─────┼────────┼─────────┼───────────┤"
        "│  n1 │ stage1 │ Failed  │ 1         │"
        "├─────┼────────┼─────────┼───────────┤"
        "│  n2 │ stage2 │ Skipped │ 2         │"
        "└─────┴────────┴─────────┴───────────┘"
        );
    }
}
