use crate::model::{Node, NodeBehavior};
use std::collections::HashMap;
use tabled::{
    Table, Tabled,
    assert::assert_table,
    settings::{Alignment, Style, object::Columns},
};

use crate::model::JobStatus;

#[derive(Tabled)]
struct Summary<'a> {
    uid: &'a str,
    stage: &'a str,
    status: &'a JobStatus,
    num_tries: u32,
}

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
