//! Limit the number of cacheable running tasks.
//!
//! Only one cacheable task of a given name is allowed to be running.
//! This helps saving resources: One task is executed and then
//! everything else uses its cached result.

use crate::model::{JobStatus, Node, NodeBehavior};
use std::collections::{HashMap, HashSet};

/// Limit the number of running cacheable tasks
pub fn limit_cached_tasks_same_name(nodemap: &mut HashMap<String, Node>) {
    let mut submitted_with_caching = find_running_cacheable_jobs(nodemap);
    for node in nodemap.values_mut() {
        if let JobStatus::ReadyForSubmission = node.status
            && let NodeBehavior::TaskNode(task) = &node.behavior
            && task.caching
        {
            if submitted_with_caching.contains(&task.name) {
                node.status = JobStatus::NotSubmitted;
            } else {
                submitted_with_caching.insert(task.name.to_string());
            }
        }
    }
}

/// Find all cacheable running task names
fn find_running_cacheable_jobs(nodemap: &HashMap<String, Node>) -> HashSet<String> {
    nodemap
        .values()
        .filter(|n| matches!(n.status, JobStatus::Running(_)))
        .filter_map(|n| {
            if let NodeBehavior::TaskNode(task) = &n.behavior
                && task.caching
            {
                Some(task.name.to_string())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Cmd, ExecMode, Task};

    // Two cacheable tasks with the same name, only one survives.
    #[test]
    fn test_limit_task_execution() {
        let mut nodemap = HashMap::from([
            (
                String::from("0"),
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
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

    // One task with the same name is running, nothing is scheduled
    #[test]
    fn test_limit_task_execution_because_running() {
        let mut nodemap = HashMap::from([
            (
                String::from("0"),
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::Running("123".to_string()),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
        ]);

        limit_cached_tasks_same_name(&mut nodemap);
        let nready = nodemap
            .values()
            .filter(|n| matches!(n.status, JobStatus::ReadyForSubmission))
            .count();
        assert_eq!(nready, 0);
    }

    // Check only cacheable running task names are detected
    #[test]
    fn test_running_cacheable_jobs() {
        let nodemap = HashMap::from([
            (
                String::from("0"),
                Node {
                    uid: String::from("0"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::Running("1".to_string()),
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("fname"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
            (
                String::from("1"),
                Node {
                    uid: String::from("1"),
                    output_artifacts: Vec::new(),
                    parents: Vec::new(),
                    children: Vec::new(),
                    status: JobStatus::ReadyForSubmission,
                    behavior: NodeBehavior::TaskNode(Task {
                        fname: String::from("not-running"),
                        name: String::from("fname"),
                        caching: true,
                        mode: ExecMode::Wrap,
                        cmd: Cmd::Sbatch,
                        try_num: 0,
                        retries: 0,
                        launch_script: String::from("lauch.sh"),
                        input_kwargs: Vec::new(),
                    }),
                },
            ),
        ]);

        let set = find_running_cacheable_jobs(&nodemap);
        assert_eq!(set, HashSet::from(["fname".to_string()]));
    }
}
