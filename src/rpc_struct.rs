use madsim::rand::{self, Rng};
use madsim::Request;
use serde::{Deserialize, Serialize};

enum Consistency {
    STRONG,
    RELAXED_READS,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<QueryResults>")]
pub struct Query {
    #[serde(with = "serde_bytes")]
    sql: Vec<u8>,
    consistency: Consistency,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResults {
    #[serde(with = "serde_bytes")]
    rows: Vec<Vec<Vec<u8>>>,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct VoteRequest {
    from_id: u64,
    term: u64,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct VoteResponse {
    from_id: u64,
    term: u64,
    vote_granted: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    id: u64,
    #[serde(with = "serde_bytes")]
    sql: Vec<u8>,
    index: u64,
    term: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct AppendEntriesRequest {
    from_id: u64,
    term: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    commit_index: u64,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct AppendEntriesResponse {
    from_id: u64,
    term: u64,
    success: bool,
    last_index: u64,
    mismatch_index: Option<u64>,
}
