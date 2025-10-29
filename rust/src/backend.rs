// $ sacct -j 20836188 --format JobID,State
// JobID             State
// ------------ ----------
// 20836188      COMPLETED
// 20836188.ba+  COMPLETED
// 20836188.ex+  COMPLETED
use crate::{JobStatus, Node};
use regex::Regex;
use std::collections::HashMap;
use std::process::Command;

pub trait Backend {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>);

    fn submit(&self, launch_script: &str, uid: &str, pipeline_dir: &str) -> String;

    fn get_running_jobs(&self, nodemap: &HashMap<String, Node>) -> HashMap<String, String> {
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
    fn check_job_status(&self, job_ids: &Vec<String>) -> String {
        if job_ids.len() == 0 {
            return String::new();
        }

        let joined_ids = job_ids.join(",");
        let output = Command::new("sacct")
            .arg("-j")
            .arg(joined_ids)
            .arg("--format")
            .arg("JobID,State")
            .output()
            .unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();
        let _stderr = String::from_utf8(output.stderr).unwrap();
        stdout
    }

    fn get_status_from_slurm_output(&self, job_id: &str, slurm_status: &str) -> JobStatus {
        if let Some(status) = self.parse_slurm_status(&job_id, &slurm_status) {
            println!("Job ID '{job_id}': status '{status}'");
            return match status.as_str() {
                "COMPLETED" => JobStatus::Completed,
                "PENDING" | "RUNNING" | "COMPLETING" => JobStatus::Running(String::from(job_id)),
                _ => JobStatus::Failed,
            };
        }
        JobStatus::Failed
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

    fn find_submitted_job_id(&self, output: &str) -> String {
        let matched = "Submitted batch job (?<jobid>\\w+)";
        let re = Regex::new(&matched).unwrap();
        match re.captures(&output) {
            Some(caps) => caps["jobid"].to_string(),
            None => panic!("Failed to identify the job id"),
        }
    }
}

impl Backend for SlurmBackend {
    fn update_status(&self, nodemap: &mut HashMap<String, Node>) {
        let job_map = self.get_running_jobs(nodemap);
        let job_ids = job_map
            .values()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let output = self.check_job_status(&job_ids);
        for (uid, job_id) in &job_map {
            let status = self.get_status_from_slurm_output(job_id, &output);
            let node = nodemap.get_mut(uid).unwrap();
            node.status = status;
        }
    }

    fn submit(&self, launch_script: &str, uid: &str, pipeline_dir: &str) -> String {
        let output = Command::new("sbatch")
            .env("SDAG_PIPELINE", pipeline_dir)
            .env("SDAG_UID", uid)
            .arg(&launch_script)
            .output()
            .expect(&format!("Failed to submit job for task {uid}"));

        let stdout = String::from_utf8(output.stdout).unwrap();
        let _stderr = String::from_utf8(output.stderr).unwrap();

        println!("{stdout}");
        self.find_submitted_job_id(&stdout)
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
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::Running(String::from("job1")),
            parents: Vec::new(),
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
            behavior: NodeBehavior::RootNode {
                children: Vec::new(),
            },
            status: JobStatus::NotSubmitted,
            parents: Vec::new(),
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
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::Running(String::from("job1")),
                parents: Vec::new(),
            },
        );
        nodemap.insert(
            String::from("2"),
            Node {
                uid: String::from("2"),
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::Running(String::from("job2")),
                parents: Vec::new(),
            },
        );

        let running_jobs = slurm.get_running_jobs(&nodemap);
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
                behavior: NodeBehavior::RootNode {
                    children: Vec::new(),
                },
                status: JobStatus::NotSubmitted,
                parents: Vec::new(),
            },
        );

        let empty: HashMap<String, String> = HashMap::new();
        assert_eq!(empty, slurm.get_running_jobs(&nodemap));
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
    #[should_panic]
    fn slurm_status_not_found() {
        let backend = SlurmBackend;
        let slurm_status = "
    JobID             State
    ------------ ----------
    20836188.ba+  COMPLETED
    20836188.ex+  COMPLETED
    ";
        backend
            .parse_slurm_status("20836188", slurm_status)
            .unwrap();
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
        let output = backend.get_status_from_slurm_output("20836188", slurm_status);
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
        let output = backend.get_status_from_slurm_output("20836188", slurm_status);
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
        let output = backend.get_status_from_slurm_output("20836188", slurm_status);
        if let JobStatus::Running(job_id) = output {
            assert_eq!(job_id, String::from("20836188"));
        } else {
            panic!("Job should be running");
        };
    }
    #[test]
    fn test_empty_job_status() {
        let backend = SlurmBackend;
        let job_ids: Vec<String> = Vec::new();
        assert_eq!(String::new(), backend.check_job_status(&job_ids));
    }

    #[test]
    fn parse_job_output() {
        let backend = SlurmBackend;
        let output = String::from("Submitted batch job 1234");
        assert_eq!(backend.find_submitted_job_id(&output), "1234");
    }

    #[test]
    fn submit() {
        let backend = SlurmBackend;
        backend.submit("submit.sh", "1", "./sdag");
    }
}
