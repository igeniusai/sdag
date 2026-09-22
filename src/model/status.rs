// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use crate::model::schemas::Parent;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::zip;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum JobType {
    Slurm(String),
    Local(u32),
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Slurm(job_id) => format!("job_id={job_id}"),
            Self::Local(pid) => format!("pid={pid}"),
        };
        write!(f, "{repr}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Completed {
    Cached,
    Branch(bool),
    Generic,
    OneOf(usize),
    Job(JobType),
}

impl fmt::Display for Completed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Generic => String::from("Completed"),
            Self::Branch(branch) => format!("Completed ({branch})"),
            Self::OneOf(uid) => format!("Completed ({uid})"),
            Self::Job(job_type) => format!("Completed ({job_type})"),
            Self::Cached => String::from("Completed (cached)"),
        };
        write!(f, "{repr}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Failed {
    Job(JobType),
    Generic,
    FailedToContact(JobType),
}

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Generic => String::from("Failed"),
            Self::Job(job_type) => format!("Failed ({job_type})"),
            Self::FailedToContact(job_type) => format!("Failed to contact ({job_type})"),
        };
        write!(f, "{repr}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Status {
    /// Node not submitted yet.
    NotSubmitted,
    /// Node not submitted yet.
    ReadyForSubmission,
    /// Node ready for submission.
    Pending(JobType),
    /// Task is running (contains the Slurm jobid).
    Running(JobType),
    /// Node completed.
    Completed(Completed),
    /// Node skipped (e.g., because of a branch).
    Skipped,
    /// Node execution failed.
    Failed(Failed),
}

impl Status {
    pub fn is_final(&self) -> bool {
        return matches!(self, Self::Completed(_) | Self::Failed(_) | Self::Skipped);
    }

    pub fn is_running(&self) -> bool {
        return matches!(self, Self::Pending(_) | Self::Running(_));
    }

    pub fn log_priority(&self) -> u16 {
        match self {
            Status::NotSubmitted => 0,
            Status::ReadyForSubmission => 1,
            Status::Skipped => 2,
            Status::Completed(_) => 3,
            Status::Failed(_) => 4,
            Status::Pending(_) => 5,
            Status::Running(_) => 6,
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let repr = match self {
            Self::NotSubmitted => String::from("Not Submitted"),
            Self::ReadyForSubmission => String::from("Ready for Submission"),
            Self::Pending(job_type) => format!("Pending ({job_type})"),
            Self::Running(job_type) => format!("Running ({job_type})"),
            Self::Completed(completed) => completed.to_string(),
            Self::Skipped => String::from("Skipped"),
            Self::Failed(failed) => failed.to_string(),
        };
        write!(f, "{repr}")
    }
}

pub fn reset_failed_or_skipped_status(statuses: &mut [Status], try_nums: &mut [usize]) {
    for (status, try_num) in zip(statuses, try_nums) {
        if matches!(status, Status::Failed(_) | Status::Skipped) {
            *status = Status::NotSubmitted;
            *try_num = 0;
        }
    }
}

pub fn is_simulation_completed(statuses: &[Status]) -> bool {
    statuses.iter().all(|s| s.is_final())
}

/// Verify that all parent have completed successfully.
pub fn all_parents_completed(parent_statuses: &[&Status]) -> bool {
    parent_statuses
        .iter()
        .all(|x| matches!(**x, Status::Completed { .. }))
}

pub fn some_parents_failed_or_skipped(parent_statuses: &[&Status]) -> bool {
    parent_statuses
        .iter()
        .any(|x| matches!(**x, Status::Skipped | Status::Failed(_)))
}

pub fn any_node_failed(statuses: &[Status]) -> bool {
    statuses.iter().any(|x| matches!(x, Status::Failed(_)))
}

/// Check if every parent has failed or has been skipped.
pub fn all_parents_failed_or_skipped(parent_statuses: &[&Status]) -> bool {
    parent_statuses
        .iter()
        .all(|x| matches!(**x, Status::Failed(_)) | matches!(**x, Status::Skipped))
}

pub fn find_completed_parent(parents: &[Parent], statuses: &[Status]) -> Option<usize> {
    for parent in parents {
        if matches!(statuses[parent.uid], Status::Completed(_)) {
            return Some(parent.uid);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::schemas::ParentKind;

    #[test]
    fn test_all_completed() {
        let s0 = Status::Completed(Completed::Cached);
        let s1 = Status::Completed(Completed::Generic);
        let s2 = Status::Completed(Completed::Job(JobType::Slurm("123".into())));
        let statuses = vec![&s0, &s1, &s2];

        assert!(all_parents_completed(&statuses));
        assert!(!some_parents_failed_or_skipped(&statuses));
        assert!(!all_parents_failed_or_skipped(&statuses));
    }

    #[test]
    fn test_one_skipped() {
        let s0 = Status::Completed(Completed::Cached);
        let s1 = Status::Skipped;
        let s2 = Status::Completed(Completed::Job(JobType::Slurm("123".into())));
        let statuses = vec![&s0, &s1, &s2];

        assert!(!all_parents_completed(&statuses));
        assert!(some_parents_failed_or_skipped(&statuses));
        assert!(!all_parents_failed_or_skipped(&statuses));
    }

    #[test]
    fn test_all_failed_or_skipped() {
        let s0 = Status::Failed(Failed::Generic);
        let s1 = Status::Skipped;
        let s2 = Status::Failed(Failed::Job(JobType::Slurm("123".into())));
        let statuses = vec![&s0, &s1, &s2];

        assert!(!all_parents_completed(&statuses));
        assert!(some_parents_failed_or_skipped(&statuses));
        assert!(all_parents_failed_or_skipped(&statuses));
    }

    #[test]
    fn test_find_completed_parent_no_parents() {
        let parents = vec![];
        let statuses = vec![];
        assert!(find_completed_parent(&parents, &statuses).is_none())
    }

    #[test]
    fn test_find_completed_parent() {
        let parents = vec![
            Parent {
                uid: 0,
                kind: ParentKind::Logical,
            },
            Parent {
                uid: 2,
                kind: ParentKind::Logical,
            },
        ];

        let s0 = Status::Completed(Completed::Cached);
        let s1 = Status::Skipped;
        let s2 = Status::Failed(Failed::Generic);
        let statuses = vec![s0, s1, s2];
        assert_eq!(find_completed_parent(&parents, &statuses).unwrap(), 0);
    }

    #[test]
    fn test_completed_parent_not_found() {
        let parents = vec![
            Parent {
                uid: 1,
                kind: ParentKind::Logical,
            },
            Parent {
                uid: 2,
                kind: ParentKind::Logical,
            },
        ];

        let s0 = Status::Completed(Completed::Cached);
        let s1 = Status::Skipped;
        let s2 = Status::Failed(Failed::Generic);
        let statuses = vec![s0, s1, s2];
        assert!(find_completed_parent(&parents, &statuses).is_none());
    }

    #[test]
    fn test_retry_run() {
        let mut statuses = vec![
            Status::Failed(Failed::Generic),
            Status::Pending(JobType::Slurm("123".into())),
            Status::Skipped,
            Status::NotSubmitted,
        ];
        let mut try_nums = vec![1, 2, 3, 0];

        reset_failed_or_skipped_status(&mut statuses, &mut try_nums);
        assert!(matches!(statuses[0], Status::NotSubmitted));
        assert!(matches!(statuses[2], Status::NotSubmitted));
        assert_eq!(try_nums[0], 0);
        assert_eq!(try_nums[2], 0);
    }

    #[test]
    fn check_simulation_is_completed() {
        let statuses = vec![
            Status::Completed(Completed::Generic),
            Status::Failed(Failed::Generic),
            Status::Skipped,
        ];

        assert!(is_simulation_completed(&statuses))
    }

    #[test]
    fn check_simulation_is_not_completed() {
        let statuses = vec![
            Status::Running(JobType::Slurm("123".into())),
            Status::Failed(Failed::Generic),
            Status::Skipped,
        ];

        assert!(!is_simulation_completed(&statuses))
    }
}
