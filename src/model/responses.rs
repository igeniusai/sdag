// SPDX-FileCopyrightText: 2026 Domyn
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqueueJob {
    pub job_id: String,
    pub job_state: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqueueResponse {
    pub jobs: Vec<SqueueJob>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctState {
    pub current: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctJob {
    pub job_id: String,
    pub state: SacctState,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SacctResponse {
    pub jobs: Vec<SacctJob>,
}
