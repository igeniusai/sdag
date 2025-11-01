use crate::model::DAG;
use serde_json;
use std::fs;
use std::io;
use std::path::PathBuf;

use env_logger::Env;
use log;
use std::env;

pub fn read_dag(path: &PathBuf) -> io::Result<DAG> {
    let buf = fs::read_to_string(path)?;
    let dag: DAG = serde_json::from_str(&buf)?;
    Ok(dag)
}

pub fn find_home_dir() -> Result<PathBuf, String> {
    match env::var("SDAG_HOME") {
        Ok(val) => Ok(PathBuf::from(val)),
        Err(_) => {
            let path = env::home_dir().ok_or(String::from(
                "Failed to identify a home directory. Please Set the \
            'SDAG_HOME' environment variable.",
            ))?;
            Ok(path.join(".sdag"))
        }
    }
}

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

    fn get_tmp_dir() -> PathBuf {
        env::temp_dir().join(Uuid::new_v4().to_string())
    }

    #[test]
    fn find_home() {
        let path = "/a/path";
        unsafe {
            env::set_var("SDAG_HOME", path);
        }
        let home_dir = find_home_dir().unwrap();
        assert_eq!(home_dir, PathBuf::from(path));
    }

    #[test]
    fn dag_parsing() {
        let tmp = get_tmp_dir();
        fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("pipeline.json");
        let contents = r#"
        {
            "name": "pipeline",
            "creation_dt": "2025-01-01 10:20:20",
            "nodes": []
        }"#;
        fs::write(&path, contents).unwrap();

        let dag = read_dag(&path).unwrap();
        assert_eq!(dag.name, "pipeline");
    }
}
