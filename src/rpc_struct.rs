use madsim::rand::{self, Rng};
use madsim::Request;
use serde::{Deserialize, Serialize};


pub type Result<T> = std::result::Result<T, String>;
#[derive(Debug, Serialize, Deserialize)]
pub enum Consistency {
    STRONG,
    RELAXED_READS,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("std::result::Result<QueryResults, crate::StoreError>")]
pub struct Query {
    #[serde(with = "serde_bytes")]
    pub sql: Vec<u8>,
    pub consistency: Consistency,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRow {
    pub values: Vec<String>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResults {
    pub rows: Vec<QueryRow>,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct VoteRequest {
    pub from_id: u64,
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct VoteResponse {
    pub from_id: u64,
    pub term: u64,
    pub vote_granted: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    #[serde(with = "serde_bytes")]
    pub sql: Vec<u8>,
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct AppendEntriesRequest {
    pub from_id: u64,
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub commit_index: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct AppendEntriesResponse {
    pub from_id: u64,
    pub term: u64,
    pub success: bool,
    pub last_index: u64,
    pub mismatch_index: Option<u64>,
}
