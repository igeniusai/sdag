//! Functions executed during the scheduler startup.

use crate::checkpoint::Checkpointer;
use crate::model::{DAG, Node};
use crate::state::StateManager;
use env_logger::Env;
use log;
use serde_json;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

pub fn prepare_dag<'a, T: StateManager>(
    dag: DAG<Node>,
    path: &PathBuf,
    checkpointer: &Checkpointer,
    state: &T,
) -> Result<DAG<Node>, Box<dyn Error>> {
    if !checkpointer.restart {
        state.prepare(&dag)?;
        state.copy_dag_into_working_dir(path)?;
        return Ok(dag);
    }

    log::info!("Loading checkpoint...");
    checkpointer.load_checkpoint(state)
}

/// Parse the DAG from the input JSON.
pub fn read_dag(path: &PathBuf) -> io::Result<DAG<Node>> {
    let buf = fs::read_to_string(path)?;
    let dag: DAG<Node> = serde_json::from_str(&buf)?;
    Ok(dag)
}

/// Find the home directory.
pub fn find_home_dir() -> Result<PathBuf, String> {
    let path = match env::var("SDAG_HOME") {
        Ok(val) => PathBuf::from(val),
        Err(_) => {
            let path = env::home_dir().ok_or(String::from(
                "Failed to identify a home directory. Please Set the \
            'SDAG_HOME' environment variable.",
            ))?;
            path
        }
    };
    Ok(path.join(".sdag"))
}

/// Configure logging.
pub fn configure_logging(log_level: &str) {
    let env = Env::default()
        .filter_or("SDAG_LOG_LEVEL", log_level)
        .write_style_or("SDAG_LOG_STYLE", "always");

    env_logger::init_from_env(env);
    log::debug!("Logging configured")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Get a temporary directory for testing purposes.
    fn get_tmp_dir() -> PathBuf {
        env::temp_dir().join(Uuid::new_v4().to_string())
    }

    /// The home directory is correctly identified.
    #[test]
    fn find_home() {
        let path = "/a/path";
        unsafe {
            env::set_var("SDAG_HOME", path);
        }
        let home_dir = find_home_dir().unwrap();
        assert_eq!(home_dir, PathBuf::from(path).join(".sdag"));
    }

    /// The DAG is correctly parsed.
    #[test]
    fn dag_parsing() {
        let tmp = get_tmp_dir();
        fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("pipeline.json");
        let contents = r#"
        {
            "meta": {
                "name": "pipeline",
                "creation_dt": "2025-01-01 10:20:20"
            },
            "nodes": []
        }"#;
        fs::write(&path, contents).unwrap();

        let dag = read_dag(&path).unwrap();
        assert_eq!(dag.meta.name, "pipeline");
    }
}
