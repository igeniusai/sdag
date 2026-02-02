//! Backend.
//!
//! The backend executes jobs and polls the status.

use crate::model::{
    Artifact, ExecMode, JobStatus, Node, NodeBehavior, NodeFailure, NodeResult, Task,
};
use crate::state::StateManager;
use log;
use regex::Regex;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::process::{Command, Output, Stdio};

/// Backend trait.
pub trait Backend {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>);
    fn submit(&self, uid: &str, task: &Task) -> Result<String, Box<dyn Error>>;
    fn submit_local(&self, uid: &str, task: &Task, artifacts: &Vec<Artifact>) -> io::Result<()>;
    fn kill_jobs(&self, job_ids: &Vec<&str>) -> io::Result<()>;
}

pub struct SchedulerBackend<'a, T: StateManager> {
    pub pipeline_name: &'a str,
    pub state: &'a T,
}

impl<'a, T: StateManager> Backend for SchedulerBackend<'a, T> {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>) {
        let status_map = self.get_slurm_status_map(&nodemap);
        for (uid, status) in status_map {
            let node = nodemap.get_mut(&uid).unwrap();
            self.assign_node_status(node, status);
        }
    }

    fn submit(&self, uid: &str, task: &Task) -> Result<String, Box<dyn Error>> {
        let mut cmd = self.build_command(uid, task)?;
        self.override_sbatch(&mut cmd, task);
        let output = cmd.arg(&task.launch_script).output()?;
        if let Ok(stderr) = String::from_utf8(output.stderr)
            && stderr.len() > 0
        {
            log::error!("Task {uid} submission: {stderr}");
        }

        let stdout = String::from_utf8(output.stdout)?;
        log::info!("Task {uid}: {stdout}");
        let job_id = self
            .find_submitted_job_id(&stdout)
            .ok_or("Job id not found")?;

        Ok(job_id)
    }

    fn submit_local(&self, uid: &str, task: &Task, artifacts: &Vec<Artifact>) -> io::Result<()> {
        self.build_command(uid, task)?
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .arg(&task.launch_script)
            .output()?;

        self.maybe_save_output_and_cache(uid, task, artifacts)
    }

    fn kill_jobs(&self, job_ids: &Vec<&str>) -> io::Result<()> {
        if job_ids.len() == 0 {
            log::warn!("No running jobs found");
            return Ok(());
        }

        let mut cmd = Command::new("scancel");
        for job in job_ids {
            cmd.arg(job);
        }

        cmd.stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()?;

        Ok(())
    }
}

impl<'a, T: StateManager> SchedulerBackend<'a, T> {
    fn get_slurm_status_map(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, JobStatus> {
        let mut jobmap: HashMap<&str, &str> = HashMap::new();
        for (uid, node) in nodemap.iter() {
            if let JobStatus::Running(job_id) = &node.status
                && let NodeBehavior::TaskNode(_) = &node.behavior
            {
                jobmap.insert(uid, job_id);
            }
        }

        let slurm_output = self.check_job_status(&jobmap);
        self.get_status_map(slurm_output, &jobmap)
    }

    fn build_command(&self, uid: &str, task: &Task) -> io::Result<Command> {
        let pipeline_dir = self.state.get_pipeline_dir();
        let mut cmd = Command::new(task.cmd.to_string());
        cmd.env("SDAG_TRY_NUM", &task.try_num.to_string())
            .env("SDAG_PIPELINE", pipeline_dir)
            .env("SDAG_UID", uid)
            .env("SDAG_TASK", &task.fname)
            .env("SDAG_TASK_NAME", &task.name);

        if let ExecMode::Ext = task.mode {
            self.set_input_as_envs(&mut cmd, uid)?;
        }

        Ok(cmd)
    }

    fn override_sbatch(&self, cmd: &mut Command, task: &Task) {
        let job_name = format!("--job-name={}", task.name);
        let error = format!("--error=./logs/{}/%x.%j.err", self.pipeline_name);
        let output = format!("--output=./logs/{}/%x.%j.out", self.pipeline_name);
        cmd.arg(&job_name).arg(&error).arg(&output);
    }

    fn set_input_as_envs(&self, cmd: &mut Command, uid: &str) -> io::Result<()> {
        let input = self.state.read_cached_input(uid)?;
        let values: HashMap<String, Value> = serde_json::from_str(&input)?;
        for (key, value) in values.iter() {
            let val = match value {
                Value::Bool(v) => v.to_string(),
                Value::String(v) => v.to_string(),
                Value::Number(v) => v.to_string(),
                Value::Null => String::new(),
                _ => serde_json::to_string(value)?,
            };
            let upper_key = key.to_uppercase();
            cmd.env(upper_key, val);
        }
        Ok(())
    }

    fn find_submitted_job_id(&self, output: &str) -> Option<String> {
        let matched = "Submitted batch job (?<jobid>\\w+)";
        let re = Regex::new(&matched).ok()?;
        let caps = re.captures(&output)?;
        Some(caps["jobid"].to_string())
    }

    fn check_job_status(&self, jobmap: &HashMap<&str, &str>) -> Result<String, Box<dyn Error>> {
        if jobmap.len() == 0 {
            return Ok(String::new());
        }

        let job_ids: Vec<&str> = jobmap.values().map(|x| *x).collect();
        let output = self.ask_status_to_slurm(job_ids)?;
        let stderr = String::from_utf8(output.stderr);
        if let Ok(msg) = stderr
            && msg.len() > 0
        {
            log::error!("Slurm stderr: {msg}")
        }

        let stdout = String::from_utf8(output.stdout)?;
        log::debug!("Slurm stdout:\n {stdout}");
        Ok(stdout)
    }

    fn ask_status_to_slurm(&self, job_ids: Vec<&str>) -> io::Result<Output> {
        let joined_ids = job_ids.join(",");
        Command::new("sacct")
            .arg("-j")
            .arg(joined_ids)
            .arg("--format")
            .arg("JobID,State")
            .output()
    }

    fn get_status_map(
        &self,
        slurm_output: Result<String, Box<dyn Error>>,
        jobmap: &HashMap<&str, &str>,
    ) -> HashMap<String, JobStatus> {
        match slurm_output {
            Ok(slurm_status) => jobmap
                .iter()
                .map(|(uid, job_id)| {
                    let status = self.extract_status(job_id, &slurm_status);
                    (uid.to_string(), status)
                })
                .collect(),

            Err(e) => {
                log::error!("Failed to contact Slurm: {e}");
                self.mark_all_jobs_as_failed(jobmap)
            }
        }
    }

    fn mark_all_jobs_as_failed(&self, jobmap: &HashMap<&str, &str>) -> HashMap<String, JobStatus> {
        log::error!("Marking all jobs as failed");
        jobmap
            .iter()
            .map(|(uid, job_id)| {
                let failure = NodeFailure::Task(job_id.to_string());
                let status = JobStatus::Failed(failure);
                (uid.to_string(), status)
            })
            .collect()
    }

    fn extract_status(&self, job_id: &str, slurm_status: &str) -> JobStatus {
        let job_id = job_id.to_string();
        match self.parse_slurm_status(&job_id, &slurm_status) {
            None => {
                log::error!("Failed to parse Slurm status");
                let failure = NodeFailure::Task(job_id);
                JobStatus::Failed(failure)
            }
            Some(status) => match status.as_str() {
                "COMPLETED" => {
                    let completed = NodeResult::Task(job_id);
                    JobStatus::Completed(completed)
                }
                "PENDING" | "RUNNING" | "COMPLETING" => JobStatus::Running(job_id),
                _ => {
                    log::error!("Failed job status {status}");
                    JobStatus::Failed(NodeFailure::Task(job_id))
                }
            },
        }
    }

    fn parse_slurm_status<'b>(&self, job_id: &'b str, slurm_status: &'b str) -> Option<String> {
        let matched = format!("{job_id}\\s+(?<status>\\w+)");
        let re = Regex::new(&matched).unwrap();
        for line in slurm_status.lines() {
            if let Some(caps) = re.captures(line) {
                let status = caps["status"].to_string();
                return Some(status);
            }
        }
        None
    }

    fn assign_node_status(&self, node: &mut Node, status: JobStatus) {
        node.status = status;
        let artifacts = &node.output_artifacts;
        if let JobStatus::Completed(NodeResult::Task(job_id)) = &node.status
            && let NodeBehavior::TaskNode(task) = &node.behavior
            && self
                .maybe_save_output_and_cache(&node.uid, task, &artifacts)
                .is_err()
        {
            log::error!("Failed to save uid {} output", node.uid);
            let failure = NodeFailure::Task(job_id.to_string());
            node.status = JobStatus::Failed(failure);
        }
    }

    fn maybe_save_output_and_cache(
        &self,
        uid: &str,
        task: &Task,
        artifacts: &Vec<Artifact>,
    ) -> io::Result<()> {
        if let ExecMode::Ext = task.mode {
            log::debug!("Saving node '{uid}' empty output");
            self.state.save_empty_output(uid, artifacts)?
        }

        if task.caching
            && let Err(e) = self.state.cache_task(&task.name, uid)
        {
            log::error!("Failed to save task '{}' cache: {}", task.name, e)
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Cmd;
    use crate::state::{LocalDirState, tests::get_tmp_dir};
    use std::fs;

    fn get_state() -> LocalDirState {
        let home_dir = get_tmp_dir();
        let pipeline_name = "pipeline";
        LocalDirState::new(&home_dir, &pipeline_name)
    }

    fn get_backend<'a>(state: &'a LocalDirState) -> SchedulerBackend<'a, LocalDirState> {
        SchedulerBackend {
            pipeline_name: "pipeline",
            state: &state,
        }
    }

    #[test]
    fn slurm_status_parsed_correctly() {
        let state = get_state();
        let backend = get_backend(&state);
        let slurm_status = "
        JobID             State
        ------------ ----------
        20836188      COMPLETED
        20836188.ba+  COMPLETED
        20836188.ex+  COMPLETED
        ";
        let output = backend
            .parse_slurm_status("20836188", slurm_status)
            .unwrap();
        assert_eq!(output, String::from("COMPLETED"));
    }

    #[test]
    fn slurm_status_not_found() {
        let state = get_state();
        let backend = get_backend(&state);
        let slurm_status = "
        JobID             State
        ------------ ----------
        20836188.ba+  COMPLETED
        20836188.ex+  COMPLETED
        ";
        let job_id = backend.parse_slurm_status("20836188", slurm_status);
        assert!(matches!(job_id, None));
    }

    #[test]
    fn get_failed_slurm_status() {
        let state = get_state();
        let backend = get_backend(&state);
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188      FAILED
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let job_id = String::from("20836188");
        let output = backend.extract_status(&job_id, slurm_status);
        assert!(matches!(output, JobStatus::Failed(NodeFailure::Task(_))));
    }

    #[test]
    fn get_unknown_slurm_status() {
        let state = get_state();
        let backend = get_backend(&state);
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let job_id = String::from("20836188");
        let output = backend.extract_status(&job_id, slurm_status);
        assert!(matches!(output, JobStatus::Failed(NodeFailure::Task(_))));
    }

    #[test]
    fn running_slurm_status() {
        let state = get_state();
        let backend = get_backend(&state);
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188      COMPLETING
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let output = backend.extract_status("20836188", slurm_status);
        if let JobStatus::Running(job_id) = output {
            assert_eq!(job_id, String::from("20836188"));
        } else {
            panic!("Job should be running");
        };
    }

    #[test]
    fn test_empty_job_status() {
        let state = get_state();
        let backend = get_backend(&state);
        let job_map: HashMap<&str, &str> = HashMap::new();
        let statuses = backend.check_job_status(&job_map).unwrap();
        assert!(statuses.is_empty());
    }

    #[test]
    fn parse_job_output() {
        let state = get_state();
        let backend = get_backend(&state);
        let output = String::from("Submitted batch job 1234");
        let job_id = backend.find_submitted_job_id(&output).unwrap();
        assert_eq!(job_id, "1234");
    }

    #[test]
    #[should_panic]
    fn submit_wrong_file() {
        let state = get_state();
        let backend = get_backend(&state);
        let task = Task {
            fname: String::from("fname"),
            name: String::from("fname"),
            caching: false,
            mode: ExecMode::Wrap,
            cmd: Cmd::Sbatch,
            try_num: 0,
            retries: 0,
            launch_script: String::from("_wrong_"),
            input_kwargs: Vec::new(),
        };
        backend.submit("1", &task).unwrap();
    }

    #[test]
    fn test_local_submission() {
        let tmp_dir = get_tmp_dir();
        fs::create_dir_all(&tmp_dir).unwrap();
        let path = tmp_dir.join("submit.sh");
        let contents = "echo hello";
        fs::write(&path, contents).unwrap();
        let launch_script = path.to_string_lossy().into_owned();
        let task = Task {
            fname: String::from("fname"),
            name: String::from("fname"),
            caching: false,
            mode: ExecMode::Wrap,
            cmd: Cmd::Bash,
            try_num: 0,
            retries: 0,
            launch_script,
            input_kwargs: Vec::new(),
        };

        let artifacts: Vec<Artifact> = Vec::new();

        let state = get_state();
        let backend = get_backend(&state);
        backend.submit_local("1", &task, &artifacts).unwrap();
    }

    #[test]
    fn test_local_task_output() {
        let tmp_dir = get_tmp_dir();
        fs::create_dir_all(&tmp_dir).unwrap();
        let path = tmp_dir.join("submit.sh");
        let contents = "echo hello";
        fs::write(&path, contents).unwrap();
        let launch_script = path.to_string_lossy().into_owned();
        let task = Task {
            fname: String::from("fname"),
            name: String::from("fname"),
            caching: false,
            mode: ExecMode::Ext,
            cmd: Cmd::Bash,
            try_num: 0,
            retries: 0,
            launch_script,
            input_kwargs: Vec::new(),
        };

        let artifacts: Vec<Artifact> = Vec::new();

        let state = get_state();
        let pipeline_dir = state.get_pipeline_dir();
        let uid = "1";
        let task_path = pipeline_dir.join(&uid);
        fs::create_dir_all(&task_path).unwrap();
        fs::write(task_path.join("input.json"), "{}").unwrap();

        let backend = get_backend(&state);
        assert!(task_path.exists());
        backend.submit_local(&uid, &task, &artifacts).unwrap();

        assert!(pipeline_dir.join(&uid).join("output.json").exists());
    }

    #[test]
    fn test_save_output_and_cache() {
        let task = Task {
            fname: String::from("fname"),
            name: String::from("fname"),
            caching: true,
            mode: ExecMode::Ext,
            cmd: Cmd::Sbatch,
            try_num: 0,
            retries: 0,
            launch_script: String::from("submit.sh"),
            input_kwargs: Vec::new(),
        };

        let state = get_state();
        let pipeline_dir = state.get_pipeline_dir();

        let uid = "1";
        let res_path = pipeline_dir.join(uid);
        let home_dir = pipeline_dir.parent().unwrap();
        let cache_path = home_dir.join(".cache").join("fname");

        fs::create_dir_all(&res_path).unwrap();
        fs::create_dir_all(&cache_path).unwrap();
        fs::write(res_path.join("input.json"), "hello").unwrap();
        fs::write(res_path.join("meta.json"), "hello").unwrap();

        let backend = get_backend(&state);
        backend
            .maybe_save_output_and_cache(uid, &task, &Vec::new())
            .unwrap();

        assert!(cache_path.join("input.json").exists());
        assert!(cache_path.join("output.json").exists());
        assert!(cache_path.join("meta.json").exists());
    }

    #[test]
    fn kill_no_jobs() {
        let state = get_state();
        let backend = get_backend(&state);
        let job_ids = Vec::new();
        backend.kill_jobs(&job_ids).unwrap();
    }
}
