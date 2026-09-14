use env_logger::Env;

use crate::schemas::DAGMeta;
use chrono::Local;
use log;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Cfg {
    pub homedir: PathBuf,
    pub dagdir: PathBuf,
    pub cachedir: PathBuf,
    pub local_cachedir: PathBuf,
    pub timestamp: String,
    pub grace_period: usize,
    pub max_dagdirs: usize,
    pub max_concurrency: usize,
    pub sleep_time: Duration,
}

impl Cfg {
    pub fn new(
        homedir: &Path,
        meta: &DAGMeta,
        max_concurrency: usize,
        time_between_polls: u64,
        grace_period: usize,
        max_dagdirs: usize,
    ) -> Self {
        let dagdir = homedir
            .join("pipelines")
            .join(&meta.pipeline_name)
            .join(&meta.hash);

        let base_cachedir = homedir.join(".cache");
        let cachedir = base_cachedir.join("global");
        let local_cachedir = base_cachedir.join("local");
        let timestamp = format!("{}", Local::now().format("%Y-%m-%dT%H:%M:%S"));
        Self {
            dagdir,
            cachedir,
            local_cachedir,
            timestamp,
            max_concurrency,
            grace_period,
            max_dagdirs,
            homedir: homedir.into(),
            sleep_time: Duration::from_secs(time_between_polls),
        }
    }

    pub fn mock_run(homedir: &Path, meta: &DAGMeta) -> Self {
        Self::new(homedir, meta, 1, 5, 1, 1)
    }
}

/// Configure logging.
pub fn configure_logging(log_level: &str) {
    let env = Env::default()
        .filter_or("SDAG_LOG_LEVEL", log_level)
        .write_style_or("SDAG_LOG_STYLE", "always");

    match env_logger::try_init_from_env(env) {
        Ok(_) => log::debug!("Logging configured"),
        Err(e) => {
            log::debug!("Logging already configured - {e}");
        }
    }
}

/// Find the home directory.
pub fn find_homedir() -> Result<PathBuf, String> {
    let path = match env::var("SDAG_HOME") {
        Ok(val) => PathBuf::from(val),
        Err(_) => {
            let path = env::home_dir().ok_or(String::from(
                "Failed to identify a home directory, please Set the \
            'SDAG_HOME' environment variable explicitely.",
            ))?;
            path
        }
    };
    Ok(path.join(".sdag"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    fn set_sdag_home() -> PathBuf {
        let path = "./a/path";
        unsafe {
            env::set_var("SDAG_HOME", path);
        }
        PathBuf::from(path)
    }

    /// The home directory is correctly identified.
    #[test]
    fn find_home() {
        let home = set_sdag_home();
        let home_dir = find_homedir().unwrap();
        assert_eq!(home_dir, PathBuf::from(home).join(".sdag"));
    }

    #[test]
    fn create_cfg() {
        let meta = DAGMeta {
            pipeline_name: "name".into(),
            hash: "xxx".into(),
            timestamp: "1900-01-01T09:20:20".into(),
            extra: Value::Null,
            import_path: String::new(),
            kwargs: HashMap::new(),
        };
        let home_path = PathBuf::from("./a/path");
        let cfg = Cfg::mock_run(&home_path, &meta);

        assert_eq!(cfg.homedir, home_path);
        assert_eq!(
            cfg.dagdir,
            home_path.join("pipelines").join("name").join("xxx")
        );
        assert_eq!(cfg.cachedir, home_path.join(".cache").join("global"));
        assert_eq!(cfg.local_cachedir, home_path.join(".cache").join("local"));
    }
}
