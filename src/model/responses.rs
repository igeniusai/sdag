// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Deserializer, Serialize};

fn default_task_id() -> String {
    String::from("0")
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArrayTaskId {
    #[serde(default = "default_task_id")]
    #[serde(deserialize_with = "job_id_as_string")]
    pub number: String,
    pub set: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctState {
    pub current: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctJob {
    #[serde(deserialize_with = "job_id_as_string")]
    pub job_id: String,
    pub state: SacctState,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctResponse {
    pub jobs: Vec<SacctJob>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqueueJob {
    #[serde(deserialize_with = "job_id_as_string")]
    pub job_id: String,
    pub job_state: Vec<String>,
    pub array_task_id: ArrayTaskId,
    #[serde(default)]
    pub array_task_string: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqueueResponse {
    pub jobs: Vec<SqueueJob>,
}

fn job_id_as_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let n = serde_json::Number::deserialize(deserializer)?;
    Ok(n.to_string())
}
