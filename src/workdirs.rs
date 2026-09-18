use crate::nodes::{Node, Task};
use crate::schemas::TaskMeta;
use crate::settings::Cfg;
use log;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub enum FileNames {
    Input,
    Output,
    Meta,
    Checkpoint,
    DAG,
    Kill,
    Script,
}

impl FileNames {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Input => "input.json",
            Self::Output => "output.json",
            Self::Meta => "meta.json",
            Self::Checkpoint => "checkpoint.json",
            Self::DAG => "dag.json",
            Self::Kill => "kill.lock",
            Self::Script => "script.sh",
        }
    }
}

pub fn find_pipeline_folder(homedir: &Path, pipeline_name: &str, hash: &str) -> Option<PathBuf> {
    let base_path = &homedir.join("pipelines").join(pipeline_name);
    if hash != "last" {
        let path = base_path.join(hash);
        return if path.exists() { Some(path) } else { None };
    };

    let subfolders = get_subfolder_creation_dates(base_path);
    if let Ok(folders) = subfolders
        && let Some(folder) = folders.into_iter().last()
    {
        Some(folder.0)
    } else {
        None
    }
}

pub fn get_subfolder_creation_dates(base_path: &Path) -> io::Result<Vec<(PathBuf, SystemTime)>> {
    let mut folders = Vec::new();
    for entry in fs::read_dir(base_path)? {
        let path = entry?.path();
        if path.is_dir() {
            log::debug!("Detected old directory {}", path.to_string_lossy());
            let meta = fs::metadata(&path)?;
            let modified = meta.modified()?;
            folders.push((path, modified))
        }
    }
    folders.sort_by_key(|x| x.1.duration_since(UNIX_EPOCH).expect("Time went backwards"));
    Ok(folders)
}

pub fn create_dir_structure(cfg: &Cfg, nodes: &[Node]) -> io::Result<()> {
    log::info!("Creating directory structure");
    let tasks = filter_tasks(nodes);

    log::info!("{} task(s) detected", tasks.len());
    create_homedir(&cfg.homedir)?;
    create_dagdir(&cfg.dagdir, cfg.max_dagdirs, &tasks)?;
    match create_cachedirs(&cfg.cachedir, &cfg.local_cachedir) {
        Ok(_) => log::info!("Caching folders successfully created"),
        Err(e) => log::error!("Cache folder creation failed {e}"),
    }

    log::info!("Directory structure creation completed");
    Ok(())
}

fn create_homedir(path: &Path) -> io::Result<()> {
    log::info!("Creating home directory in {}", path.to_string_lossy());
    fs::create_dir_all(path)
}

fn create_cachedirs(cachedir: &Path, local_cachedir: &Path) -> io::Result<()> {
    log::info!("Creating cache dir in {}", cachedir.to_string_lossy());
    fs::create_dir_all(cachedir)?;

    log::info!(
        "Creating local cache dir in {}",
        local_cachedir.to_string_lossy()
    );
    fs::create_dir_all(local_cachedir)
}

fn create_dagdir(path: &Path, max_dagdirs: usize, tasks: &[&Task]) -> io::Result<()> {
    if path.exists() {
        let path_str = path.to_string_lossy();
        log::warn!("DAG directory {} already exists, removing...", path_str);
        fs::remove_dir_all(&path)?;
    }

    if let Some(parent_path) = path.parent()
        && parent_path.exists()
    {
        let subfolders = get_subfolder_creation_dates(parent_path)?;
        let nsubfolders = subfolders.len();
        log::debug!("Number of older DAG directories: {}", nsubfolders);

        let ndel = 1 + nsubfolders as i64 - max_dagdirs as i64;
        if max_dagdirs > 0 && ndel > 0 {
            log::warn!("Number of older DAG directories to be deleted: {}", ndel);
            for (dir, _) in &subfolders[..ndel as usize] {
                log::info!("Removing {}", dir.to_string_lossy());
                fs::remove_dir_all(dir)?;
            }
        }
    }

    fs::create_dir_all(&path)?;
    create_node_dirs(&path, tasks)?;
    Ok(())
}

fn create_node_dirs(base_path: &Path, tasks: &[&Task]) -> io::Result<()> {
    for task in tasks {
        let path = base_path.join(task.uid.to_string());
        log::debug!(
            "Creating task '{}' directory in {}",
            task.uid,
            path.to_string_lossy()
        );

        fs::create_dir(&path)?;
        write_meta(&path, task)?;
    }

    Ok(())
}

fn write_meta(path: &Path, task: &Task) -> Result<(), io::Error> {
    log::debug!(
        "Writing task '{}' metadata in {}",
        task.uid,
        path.to_string_lossy()
    );
    let meta = TaskMeta {
        fn_name: task.fn_name.to_string(),
        name: task.name.to_string(),
    };
    let content = serde_json::to_string(&meta)?;
    let dst = path.join(FileNames::Meta.as_str());
    fs::write(dst, &content)
}

fn filter_tasks(nodes: &[Node]) -> Vec<&Task> {
    nodes
        .iter()
        .filter_map(|n| {
            if let Node::Task(task) = n {
                Some(task)
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::Root;
    use crate::schemas::{
        Cmd, DAGMeta, ExecMode, Parent, ParentKind, Scope, Script, ScriptPath, SlurmOverride,
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::env;
    use uuid::Uuid;

    fn get_tmp_dir() -> PathBuf {
        let path = env::temp_dir().join(Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn get_task() -> Task {
        Task {
            uid: 1,
            parents: vec![Parent {
                uid: 0,
                kind: ParentKind::Logical,
            }],
            scope: Scope::Global,
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: true,
            cache_ignore: vec![],
            cache_size: 1,
            mode: ExecMode::Wrap,
            cmd: Cmd::Sbatch,
            retries: 0,
            envs: HashMap::new(),
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            tags: vec![],
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm: SlurmOverride::default(),
        }
    }

    fn get_nodes() -> Vec<Node> {
        let root = Root {
            uid: 0,
            pipeline_name: "pipeline".into(),
            parents: Vec::new(),
            children: Vec::new(),
        };
        let task = get_task();
        vec![Node::Root(root), Node::Task(task)]
    }

    fn create_subfolders(path: &Path) {
        let path3 = path.join("3");
        let path1 = path.join("1");
        let path2 = path.join("2");
        fs::create_dir_all(&path3).unwrap();
        fs::create_dir_all(&path1).unwrap();
        fs::create_dir_all(&path2).unwrap();
    }

    #[test]
    fn test_filter_task() {
        let nodes = get_nodes();
        let tasks = filter_tasks(&nodes);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].uid, 1);
    }

    #[test]
    fn test_create_node_dirs() {
        let path = get_tmp_dir();
        let task = get_task();
        let tasks = vec![&task];
        create_node_dirs(&path, &tasks).unwrap();

        let meta_path = path.join("1").join("meta.json");
        let meta_str = fs::read_to_string(&meta_path).unwrap();
        let meta: TaskMeta = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta.fn_name, "fn_name");
        assert_eq!(meta.name, "name");
    }

    #[test]
    #[ignore]
    fn test_get_subfolder_creation_dates() {
        let path = get_tmp_dir();
        create_subfolders(&path);
        let subfolders = get_subfolder_creation_dates(&path).unwrap();
        assert_eq!(subfolders[0].0, path.join("3"));
        assert_eq!(subfolders[1].0, path.join("1"));
        assert_eq!(subfolders[2].0, path.join("2"));
    }

    #[test]
    fn test_create_dagdirs_with_existing_dir() {
        let path = get_tmp_dir();
        create_subfolders(&path);

        // Create a file to check deletion
        let dir_path = path.join("2");
        let file_path = dir_path.join("file");
        fs::File::create(&file_path).unwrap();

        let task = get_task();
        let tasks = vec![&task];
        let task_path = dir_path.join(task.uid.to_string());

        create_dagdir(&dir_path, 3, &tasks).unwrap();

        assert!(!file_path.exists());
        assert!(task_path.exists());
        assert!(path.join("1").exists());
        assert!(path.join("3").exists());
    }

    #[test]
    #[ignore]
    fn test_create_dagdirs_above_max() {
        let path = get_tmp_dir();
        create_subfolders(&path);

        let dir_path = path.join("4");
        let task = get_task();
        let tasks = vec![&task];
        let task_path = dir_path.join(task.uid.to_string());

        create_dagdir(&dir_path, 2, &tasks).unwrap();

        assert!(task_path.exists());
        assert!(path.join("2").exists());
        assert!(!path.join("3").exists());
        assert!(!path.join("1").exists());
    }

    #[test]
    fn test_create_dir_structure() {
        let path = get_tmp_dir();
        let nodes = get_nodes();
        let meta = DAGMeta {
            pipeline_name: "pipeline".into(),
            hash: "xxx".into(),
            timestamp: "1900-01-01T09:20:20".into(),
            extra: Value::Null,
            import_path: String::new(),
            kwargs: HashMap::new(),
        };

        let homedir = path.join("home");
        let cfg = Cfg::new(&homedir, &meta, "info", 1, 5, 1, 1, false);
        create_dir_structure(&cfg, &nodes).unwrap();

        assert!(cfg.homedir.exists());
        assert!(cfg.cachedir.exists());
        assert!(cfg.local_cachedir.exists());
        assert!(cfg.dagdir.exists());
    }
}
