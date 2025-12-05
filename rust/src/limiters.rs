use crate::model::{JobStatus, Node, NodeBehavior};
use std::collections::{HashMap, HashSet};

pub fn limit_cached_tasks_same_name(nodemap: &mut HashMap<String, Node>) {
    let mut submitted_with_caching: HashSet<String> = HashSet::new();
    for node in nodemap.values_mut() {
        if let JobStatus::ReadyForSubmission = node.status
            && let NodeBehavior::TaskNode { caching, fname, .. } = &node.behavior
            && *caching
        {
            if submitted_with_caching.contains(fname) {
                node.status = JobStatus::NotSubmitted;
            } else {
                submitted_with_caching.insert(fname.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_limit_task_execution() {
        let mut nodemap = HashMap::from([
            (
                String::from("0"),
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    output_used: false,
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode {
                        fname: String::from("fname"),
                        caching: true,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    },
                },
            ),
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    output_used: false,
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode {
                        fname: String::from("fname"),
                        caching: true,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    },
                },
            ),
        ]);

        limit_cached_tasks_same_name(&mut nodemap);
        let nready = nodemap
            .values()
            .filter(|n| matches!(n.status, JobStatus::ReadyForSubmission))
            .count();
        assert_eq!(nready, 1);
    }
}
