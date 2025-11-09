use crate::model::{JobStatus, Node, NodeResult};
use log;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Output};

pub trait Backend {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>);
    fn submit(
        &self,
        launch_script: &str,
        uid: &str,
        pipeline_dir: &PathBuf,
        try_num: &u32,
    ) -> Result<String, Box<dyn Error>>;

    fn get_running_job_ids(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, String> {
        nodemap
            .values()
            .filter_map(|n| self.get_job_id_if_running(n))
            .collect()
    }

    fn get_job_id_if_running(&self, node: &Node) -> Option<(String, String)> {
        if let JobStatus::Running(job_id) = &node.status {
            Some((node.uid.to_string(), job_id.to_string()))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlurmBackend;
impl SlurmBackend {
    fn check_job_status(
        &self,
        job_map: &HashMap<String, String>,
    ) -> Result<String, Box<dyn Error>> {
        let job_ids: Vec<&str> = job_map.values().map(String::as_str).collect();
        if job_ids.len() == 0 {
            return Ok(String::new());
        }

        let output = self.ask_status_to_slurm(job_ids)?;
        let stderr = String::from_utf8(output.stderr);
        if let Ok(msg) = stderr {
            if msg.len() > 0 {
                log::error!("Slurm stderr: {msg}")
            }
        };

        let stdout = String::from_utf8(output.stdout)?;
        log::debug!("Slurm stdout:\n {stdout}");
        Ok(stdout)
    }

    fn extract_status(&self, job_id: &str, slurm_status: &str) -> JobStatus {
        if let Some(status) = self.parse_slurm_status(&job_id, &slurm_status) {
            log::info!("Job ID '{job_id}': status '{status}'");
            return match status.as_str() {
                "COMPLETED" => {
                    let completed = NodeResult::Task(String::from(job_id));
                    JobStatus::Completed(completed)
                }
                "PENDING" | "RUNNING" | "COMPLETING" => {
                    let job = String::from(job_id);
                    JobStatus::Running(job)
                }
                _ => JobStatus::Failed,
            };
        }
        JobStatus::Failed
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

    fn parse_slurm_status<'a>(&self, job_id: &'a str, slurm_status: &'a str) -> Option<String> {
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

    fn find_submitted_job_id(&self, output: &str) -> Option<String> {
        let matched = "Submitted batch job (?<jobid>\\w+)";
        let re = Regex::new(&matched).ok()?;
        let caps = re.captures(&output)?;
        Some(caps["jobid"].to_string())
    }

    fn get_status_map<'a>(
        &'a self,
        slurm_output: Result<String, Box<dyn Error>>,
        job_map: &'a HashMap<String, String>,
    ) -> HashMap<&'a String, JobStatus> {
        match slurm_output {
            Ok(output) => job_map
                .iter()
                .map(|(uid, job_id)| (uid, self.extract_status(job_id, &output)))
                .collect(),
            Err(_) => {
                log::error!("Failed to contact Slurm, marking all running jobs as failed");
                job_map.keys().map(|uid| (uid, JobStatus::Failed)).collect()
            }
        }
    }
}

impl Backend for SlurmBackend {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>) {
        let job_map = self.get_running_job_ids(nodemap);
        let slurm_output = self.check_job_status(&job_map);
        let status_map = self.get_status_map(slurm_output, &job_map);

        for (uid, status) in status_map {
            let node = nodemap.get_mut(uid).unwrap();
            node.status = status;
        }
    }

    fn submit(
        &self,
        launch_script: &str,
        uid: &str,
        pipeline_dir: &PathBuf,
        try_num: &u32,
    ) -> Result<String, Box<dyn Error>> {
        let try_num_str = try_num.to_string();
        let output = Command::new("sbatch")
            .env("SDAG_TRY_NUM", &try_num_str)
            .env("SDAG_PIPELINE", pipeline_dir)
            .env("SDAG_UID", uid)
            .arg(&launch_script)
            .output()?;

        if let Ok(stderr) = String::from_utf8(output.stderr) {
            if stderr.len() > 0 {
                log::error!("Task {uid} submission: {stderr}");
            }
        }

        let stdout = String::from_utf8(output.stdout)?;
        log::info!("Task {uid}: {stdout}");
        let job_id = self
            .find_submitted_job_id(&stdout)
            .ok_or("Job id not found")?;
        Ok(job_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeBehavior;

    #[test]
    fn get_running_job_id() {
        let slurm = SlurmBackend;
        let node = Node {
            uid: String::from("1"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::Running(String::from("job1")),
            parents: Vec::new(),
            children: Vec::new(),
        };

        let res = slurm.get_job_id_if_running(&node);
        assert_eq!(res.unwrap(), (String::from("1"), String::from("job1")));
    }

    #[test]
    #[should_panic]
    fn get_no_job_id() {
        let slurm = SlurmBackend;
        let node = Node {
            uid: String::from("1"),
            behavior: NodeBehavior::RootNode,
            status: JobStatus::NotSubmitted,
            parents: Vec::new(),
            children: Vec::new(),
        };

        slurm.get_job_id_if_running(&node).unwrap();
    }

    #[test]
    fn get_running_jobs() {
        let slurm = SlurmBackend;
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("1"),
            Node {
                uid: String::from("1"),
                behavior: NodeBehavior::RootNode,
                status: JobStatus::Running(String::from("job1")),
                parents: Vec::new(),
                children: Vec::new(),
            },
        );
        nodemap.insert(
            String::from("2"),
            Node {
                uid: String::from("2"),
                behavior: NodeBehavior::RootNode,
                status: JobStatus::Running(String::from("job2")),
                parents: Vec::new(),
                children: Vec::new(),
            },
        );

        let running_jobs = slurm.get_running_job_ids(&nodemap);
        assert_eq!(
            HashMap::from([
                (String::from("1"), String::from("job1")),
                (String::from("2"), String::from("job2"))
            ]),
            running_jobs
        );
    }

    #[test]
    fn get_no_running_jobs() {
        let slurm = SlurmBackend;
        let mut nodemap = HashMap::new();
        nodemap.insert(
            String::from("1"),
            Node {
                uid: String::from("1"),
                behavior: NodeBehavior::RootNode,
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
                children: Vec::new(),
            },
        );

        assert!(slurm.get_running_job_ids(&nodemap).is_empty());
    }

    #[test]
    fn slurm_status_parsed_correctly() {
        let backend = SlurmBackend;
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
        let backend = SlurmBackend;
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
        let backend = SlurmBackend;
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188      FAILED
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let output = backend.extract_status("20836188", slurm_status);
        assert!(matches!(output, JobStatus::Failed));
    }

    #[test]
    fn get_unknown_slurm_status() {
        let backend = SlurmBackend;
        let slurm_status = "
            JobID             State
            ------------ ----------
            20836188.ba+  COMPLETED
            20836188.ex+  COMPLETED
            ";
        let output = backend.extract_status("20836188", slurm_status);
        assert!(matches!(output, JobStatus::Failed));
    }

    #[test]
    fn running_slurm_status() {
        let backend = SlurmBackend;
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
        let backend = SlurmBackend;
        let job_map: HashMap<String, String> = HashMap::new();
        let statuses = backend.check_job_status(&job_map).unwrap();
        assert!(statuses.is_empty());
    }

    #[test]
    fn parse_job_output() {
        let backend = SlurmBackend;
        let output = String::from("Submitted batch job 1234");
        let job_id = backend.find_submitted_job_id(&output).unwrap();
        assert_eq!(job_id, "1234");
    }

    #[test]
    #[should_panic]
    fn submit_wrong_file() {
        let backend = SlurmBackend;
        let pipeline_dir = PathBuf::from("./sdag");
        let try_num = 1;
        backend
            .submit("_wrong_", "1", &pipeline_dir, &try_num)
            .unwrap();
    }
}
