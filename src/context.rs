use crate::nodes::Node;
use crate::status::{Failed, JobType, Status};
use crate::submission;
use log;
use std::collections::{HashSet, VecDeque};
use std::process::Child;

pub enum Job {
    Task(usize),
    OneOf(usize, usize),
    Branch(usize),
    ValidateCache(usize),
    SaveCache(usize),
    SaveExtOutput(usize, JobType),
}

pub struct Ctx {
    pub updated: VecDeque<usize>,
    pub statuses: Vec<Status>,
    pub jobs: VecDeque<Job>,
    pub try_nums: Vec<usize>,
    pub local_jobs: Vec<(usize, Child)>,
    pub slurm_jobs: Vec<(usize, String)>,
    pub running_cacheable: HashSet<String>,
}
impl Ctx {
    pub fn new(nodes: &[Node]) -> Result<Self, String> {
        let nnodes = nodes.len();
        let min = nodes.iter().map(|n| n.get_uid()).min();
        let max = nodes.iter().map(|n| n.get_uid()).max();
        let Some(min_uid) = min else {
            return Err("Failed to compute min uid".to_string());
        };
        let Some(max_uid) = max else {
            return Err("Failed to compute max uid".to_string());
        };
        if min_uid != 0 || max_uid != nnodes - 1 {
            let err = format!(
                "Min uid '{}', max uid '{}', and number of nodes '{}' do not correspond.",
                min_uid, max_uid, nnodes
            );
            return Err(err);
        };

        let mut statuses = Vec::with_capacity(nnodes);
        for _ in 0..nnodes {
            statuses.push(Status::NotSubmitted);
        }

        Ok(Self {
            statuses,
            updated: VecDeque::new(),
            jobs: VecDeque::new(),
            try_nums: vec![0; nnodes],
            running_cacheable: HashSet::new(),
            local_jobs: Vec::new(),
            slurm_jobs: Vec::new(),
        })
    }

    pub fn from_checkpoint(nodes: &[Node], statuses: Vec<Status>, try_nums: Vec<usize>) -> Self {
        let mut ctx = Self {
            statuses,
            try_nums,
            updated: VecDeque::new(),
            jobs: VecDeque::new(),
            running_cacheable: HashSet::new(),
            local_jobs: Vec::new(),
            slurm_jobs: Vec::new(),
        };
        ctx.mark_all_local_running_jobs_as_failed(nodes);
        ctx.collect_running_slurm_jobs();
        ctx
    }

    pub fn nrunning(&self) -> usize {
        self.slurm_jobs.len() + self.local_jobs.len()
    }

    fn mark_all_local_running_jobs_as_failed(&mut self, nodes: &[Node]) {
        for node in nodes {
            if let Node::Task(task) = node
                && let Status::Running(JobType::Local(pid)) = self.statuses[task.uid]
            {
                log::warn!("Marking running local task {} as failed.", task.uid);
                let failure = Failed::Job(JobType::Local(pid));
                submission::handle_retries(task, self, failure);
            }
        }
    }

    fn collect_running_slurm_jobs(&mut self) {
        for (uid, status) in self.statuses.iter().enumerate() {
            {
                if let Status::Pending(JobType::Slurm(job_id))
                | Status::Running(JobType::Slurm(job_id)) = status
                {
                    log::debug!("Task {}: Collecting Slurm Job id '{job_id}'", uid);
                    self.slurm_jobs.push((uid, job_id.to_string()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::Task;
    use crate::schemas::{Cmd, ExecMode, Script, ScriptPath, SlurmOverride};
    use std::collections::HashMap;

    fn get_tasks() -> Vec<Task> {
        let task0 = Task {
            uid: 0,
            parents: vec![],
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: true,
            cache_local: false,
            cache_ignore: vec![],
            mode: ExecMode::Wrap,
            cmd: Cmd::Bash,
            retries: 0,
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            envs: HashMap::new(),
            tags: vec![],
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm_override: SlurmOverride::new(),
        };

        let mut task1 = task0.clone();
        let mut task2 = task0.clone();
        task1.uid = 1;
        task2.uid = 2;
        task1.cmd = Cmd::Sbatch;
        task2.cmd = Cmd::Sbatch;

        vec![task0, task1, task2]
    }

    fn get_nodes(tasks: Vec<Task>) -> Vec<Node> {
        tasks.into_iter().map(|t| Node::Task(t)).collect()
    }

    #[test]
    #[should_panic]
    fn fail_min_uid_check() {
        let mut tasks = get_tasks();
        tasks[0].uid = 1;
        let nodes = get_nodes(tasks);
        Ctx::new(&nodes).unwrap();
    }

    #[test]
    #[should_panic]
    fn fail_max_uid_check() {
        let mut tasks = get_tasks();
        tasks[2].uid = 3;
        let nodes = get_nodes(tasks);
        Ctx::new(&nodes).unwrap();
    }

    #[test]
    fn test_checkpoint_load() {
        let tasks = get_tasks();
        let nodes = get_nodes(tasks);

        let try_nums = vec![1, 1, 1];
        let statuses = vec![
            Status::Running(JobType::Local(123)),
            Status::Running(JobType::Slurm("123".into())),
            Status::Pending(JobType::Slurm("456".into())),
        ];

        let ctx = Ctx::from_checkpoint(&nodes, statuses, try_nums);
        assert!(matches!(ctx.statuses[0], Status::Failed(_)));
        assert_eq!(ctx.slurm_jobs.len(), 2);
    }
}
