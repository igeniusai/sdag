// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Deserializer, Serialize};

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
