//! Backend.
//!
//! The backend executes jobs and polls the status.

use crate::nodes::Task;
use crate::schemas::{DAGMeta, ExecMode, Script, SlurmOverride};
use crate::settings::Cfg;
use crate::state;
use crate::workdirs::FileNames;
use log;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::process::{Child, Command, Output, Stdio};

pub fn submit_slurm(
    task: &Task,
    try_num: usize,
    cfg: &Cfg,
    meta: &DAGMeta,
) -> Result<String, Box<dyn Error>> {
    let input = state::read_input_from_parents(task, &cfg.dagdir)?;
    let mut cmd = build_command(task, try_num, cfg, meta, &input, &task.envs)?;
    save_input(&input, task, cfg)?;
    let (output, error) = find_output_and_error_paths(
        &task.slurm_override,
        &meta.pipeline_name,
        &task.pipeline_name,
        &cfg.timestamp,
    );
    state::create_output_and_error_log_dirs(&output, &error);
    override_sbatch(&mut cmd, &task.slurm_override, &task.name, &output, &error);
    set_script_path(&mut cmd, task, cfg)?;
    let output = cmd.output()?;

    if let Ok(stderr) = String::from_utf8(output.stderr)
        && stderr.len() > 0
    {
        log::error!("Task '{}' submission:\n{stderr}", task.uid);
    }

    let stdout = String::from_utf8(output.stdout)?;
    log::info!("Task '{}':\n{stdout}", task.uid);

    if !output.status.success() {
        log::error!("Task '{}' returned non-zero exit status", task.uid);
        return Err("non-zero exit status".into());
    }

    let job_id = find_submitted_job_id(&stdout).ok_or("Job id not found")?;
    Ok(job_id)
}

pub fn submit_local(
    task: &Task,
    try_num: usize,
    cfg: &Cfg,
    meta: &DAGMeta,
) -> Result<Child, Box<dyn Error>> {
    log::info!("Submitting local task '{}'", task.uid);
    let input = state::read_input_from_parents(task, &cfg.dagdir)?;
    let mut cmd = build_command(task, try_num, cfg, meta, &input, &task.envs)?;
    set_script_path(&mut cmd, task, cfg)?;
    save_input(&input, task, cfg)?;

    cmd.stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| e.into())
}

pub fn submit_local_blocking(
    task: &Task,
    try_num: usize,
    cfg: &Cfg,
    meta: &DAGMeta,
) -> Result<Output, Box<dyn Error>> {
    log::info!("Submitting local task '{}'", task.uid);
    let input = state::read_input_from_parents(task, &cfg.dagdir)?;
    let mut cmd = build_command(task, try_num, cfg, meta, &input, &task.envs)?;
    set_script_path(&mut cmd, task, cfg)?;
    save_input(&input, task, cfg)?;

    cmd.stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| e.into())
}

fn build_command(
    task: &Task,
    try_num: usize,
    cfg: &Cfg,
    meta: &DAGMeta,
    input: &HashMap<String, Value>,
    envs: &HashMap<String, Value>,
) -> io::Result<Command> {
    let input_kwargs = serde_json::to_string(&meta.kwargs)?;
    let mut cmd = Command::new(task.cmd.to_string());

    for (env_name, env_val) in envs.iter() {
        cmd.env(env_name, convert_env_to_string(env_val));
    }

    if let ExecMode::Ext = task.mode {
        set_input_as_envs(&mut cmd, &input);
    }

    cmd.env("SDAG_TRY_NUM", try_num.to_string())
        .env("SDAG_PIPELINE_DIR", &cfg.dagdir)
        .env("SDAG_PIPELINE_NAME", &meta.pipeline_name)
        .env("SDAG_INPUT_KWARGS", &input_kwargs)
        .env("SDAG_SUBPIPELINE_NAME", &task.pipeline_name)
        .env("SDAG_IMPORT_PATH", &meta.import_path)
        .env("SDAG_UID", task.uid.to_string())
        .env("SDAG_TASK_FN", &task.fn_name)
        .env("SDAG_TASK_NAME", &task.name);

    Ok(cmd)
}

fn save_input(input: &HashMap<String, Value>, task: &Task, cfg: &Cfg) -> io::Result<()> {
    let filename = FileNames::Input.as_str();
    let dir = task.uid.to_string();
    let path = cfg.dagdir.join(dir).join(filename);
    state::save_input(input, &path)
}

fn override_sbatch(
    cmd: &mut Command,
    slurm_override: &SlurmOverride,
    name: &str,
    output: &str,
    error: &str,
) {
    cmd.arg(&format!("--error={error}"));
    cmd.arg(&format!("--output={output}"));

    let mut job_name = name;
    if let Some(user_job_name) = &slurm_override.job_name {
        job_name = user_job_name;
    }
    cmd.arg(&format!("--job-name={job_name}"));

    if let Some(nodes) = &slurm_override.nodes {
        cmd.arg(&format!("--nodes={nodes}"));
    }
    if let Some(partition) = &slurm_override.partition {
        cmd.arg(&format!("--partition={partition}"));
    }
    if let Some(qos) = &slurm_override.qos {
        cmd.arg(&format!("--qos={qos}"));
    }
    if let Some(gpus_per_node) = &slurm_override.gpus_per_node {
        cmd.arg(&format!("--gpus-per-node={gpus_per_node}"));
    }
    if let Some(ntasks_per_node) = &slurm_override.ntasks_per_node {
        cmd.arg(&format!("--ntasks-per-node={ntasks_per_node}"));
    }
    if let Some(account) = &slurm_override.account {
        cmd.arg(&format!("--account={account}"));
    }
    if let Some(cpus_per_task) = &slurm_override.cpus_per_task {
        cmd.arg(&format!("--cpus-per-task={cpus_per_task}"));
    }
    if let Some(mem) = &slurm_override.mem {
        cmd.arg(&format!("--mem={mem}"));
    }
    if let Some(time) = &slurm_override.time {
        cmd.arg(&format!("--time={time}"));
    }
}

fn find_output_and_error_paths(
    slurm_override: &SlurmOverride,
    pipeline: &str,
    subpipeline: &str,
    timestamp: &str,
) -> (String, String) {
    let base = format!("./logs/{pipeline}/{timestamp}/{subpipeline}");
    let mut output = format!("{base}/%x.%j.out");
    if let Some(user_output) = &slurm_override.output {
        output = user_output.to_string()
    };

    let mut error = format!("{base}/%x.%j.err");
    if let Some(user_error) = &slurm_override.error {
        error = user_error.to_string();
    }

    (output, error)
}

fn set_script_path(cmd: &mut Command, task: &Task, cfg: &Cfg) -> io::Result<()> {
    let path = match &task.script {
        Script::Script(script) => {
            let filename = FileNames::Script.as_str();
            let dir = task.uid.to_string();
            let path = cfg.dagdir.join(dir).join(filename);
            state::save_script(&script.content, &path)?;
            &path.to_string_lossy().into()
        }
        Script::ScriptPath(script_path) => &script_path.path,
    };

    cmd.arg(&path);
    Ok(())
}

fn set_input_as_envs(cmd: &mut Command, input: &HashMap<String, Value>) {
    for (key, value) in input.iter() {
        let val = convert_env_to_string(value);
        let upper_key = key.to_uppercase();
        cmd.env(upper_key, val);
    }
}

fn convert_env_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(v) => v.to_string(),
        _ => value.to_string(),
    }
}

fn find_submitted_job_id(output: &str) -> Option<String> {
    let matched = "Submitted batch job (?<jobid>\\w+)";
    let re = Regex::new(&matched).ok()?;
    let caps = re.captures(&output)?;
    Some(caps["jobid"].into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::Cmd;
    use crate::schemas::ScriptContent;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn get_tmp_dir() -> PathBuf {
        let path = env::temp_dir().join(Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn get_meta() -> DAGMeta {
        DAGMeta {
            pipeline_name: "pipe".into(),
            hash: "xxx".into(),
            timestamp: "1920-01-01T09:20:20".into(),
            extra: Value::Null,
            import_path: String::new(),
            kwargs: HashMap::new(),
        }
    }

    fn get_cfg() -> Cfg {
        let meta = get_meta();
        let homedir = get_tmp_dir();
        Cfg::new(&homedir, &meta, 5, 4)
    }

    fn get_task() -> Task {
        Task {
            uid: 0,
            parents: vec![],
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: false,
            cache_local: false,
            cache_ignore: vec![],
            mode: ExecMode::Wrap,
            cmd: Cmd::Bash,
            retries: 0,
            script: Script::Script(ScriptContent {
                content: "echo hello".into(),
            }),
            envs: HashMap::from([("VAR".into(), Value::Bool(true))]),
            tags: vec![],
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm_override: SlurmOverride::new(),
        }
    }

    #[test]
    fn save_script() {
        let mut cmd = Command::new("ls");
        let task = get_task();
        let cfg = get_cfg();
        let task_dir = cfg.dagdir.join(task.uid.to_string());
        fs::create_dir_all(&task_dir).unwrap();
        set_script_path(&mut cmd, &task, &cfg).unwrap();

        let script_path = task_dir.join(FileNames::Script.as_str());
        assert!(script_path.is_file())
    }

    #[test]
    fn parse_job_output() {
        let output = "Submitted batch job 1234".to_string();
        let job_id = find_submitted_job_id(&output).unwrap();
        assert_eq!(job_id, "1234");
    }

    #[test]
    fn test_save_input() {
        let input = HashMap::from([("a".to_string(), Value::Null)]);
        let task = get_task();
        let cfg = get_cfg();
        let task_dir = cfg.dagdir.join(task.uid.to_string());
        fs::create_dir_all(&task_dir).unwrap();
        save_input(&input, &task, &cfg).unwrap();

        let input_path = task_dir.join(FileNames::Input.as_str());
        assert!(input_path.exists());
    }

    #[test]
    fn test_submit_local() {
        let meta = get_meta();
        let mut task = get_task();
        task.script = Script::Script(ScriptContent {
            content: "true".into(),
        });
        let cfg = get_cfg();
        let task_dir = cfg.dagdir.join(task.uid.to_string());
        fs::create_dir_all(&task_dir).unwrap();

        let mut child = submit_local(&task, 1, &cfg, &meta).unwrap();
        let status = child.wait().unwrap();
        assert!(status.success());
    }

    #[test]
    fn find_default_log_paths() {
        let slurm_override = SlurmOverride::default();
        let (output, error) =
            find_output_and_error_paths(&slurm_override, "pipe", "subpipe", "1900-01-01");

        assert_eq!(output, "./logs/pipe/1900-01-01/subpipe/%x.%j.out");
        assert_eq!(error, "./logs/pipe/1900-01-01/subpipe/%x.%j.err");
    }

    #[test]
    fn find_custom_log_paths() {
        let mut slurm_override = SlurmOverride::default();
        slurm_override.output = Some("output.out".into());
        slurm_override.error = Some("error.err".into());

        let (output, error) =
            find_output_and_error_paths(&slurm_override, "pipe", "subpipe", "1900-01-01");

        assert_eq!(output, "output.out");
        assert_eq!(error, "error.err");
    }
}
