//! Moudle for store paramater into file

use std::{
    ffi::OsStr,
    fs::{File, OpenOptions, self},
    io::{BufRead, BufReader, Write},
    os::unix::prelude::AsRawFd,
    path::Path,
};

use crate::server::StoreCommand;
use little_raft::message::LogEntry;
use little_raft::state_machine::HardState;
use little_raft::state_machine::Storage;
use log::info;
use nix::unistd;
use serde::{Deserialize, Serialize};
/*
use serde::ser::{Serialize, Serializer, SerializeStruct};

use little_raft::state_machine::StateMachineTransition;

struct BadLogEntry<T> (LogEntry<T>)
    where T: StateMachineTransition;


impl<T> Serialize for BadLogEntry<T>
    where
    T: StateMachineTransition,
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BadLogEntry$StoreCommand$", 3)?;
        state.serialize_field("transition", &self.0.transition);
        state.serialize_field("index", &self.0.index);
        state.serialize_field("term", &self.0.term);
        state.end()
    }
}
*/

#[derive(Debug, Serialize, Deserialize)]
struct MyLogEntry {
    transition: StoreCommand,
    index: usize,
    term: usize,
}

/// FileStore
///
/// to store value need persisted
#[derive(Debug)]
pub struct FileStore {
    //path for store
    path: String,
    //hardstate file
    var_file: File,
    log_file: Option<File>,
    cur_term: usize,
    cur_vote: Option<usize>,
    first_index: usize,
    last_index: usize,
}

impl FileStore {
    /// get a new FileStore
    /// db file will be clear
    pub fn new(path: Option<String>) -> Self {
        println!("Store create!");
        let path = match path {
            Some(path) => path,
            None => String::from("."),
        };
        let p = path.clone() + "/HardState/data.txt";
        let var_path = Path::new(OsStr::new(p.as_str()));
        let var_file = OpenOptions::new().read(true).open(var_path);
        let (cur_term, cur_vote) = match var_file {
            Ok(var_file) => {
                //let mut buf = String::new();
                //var_file.read_to_string(& mut buf).unwrap();
                let reader = BufReader::new(var_file);
                let a = reader.lines().collect::<Vec<_>>();
                let mut has = false;
                let mut hs: HardState = HardState {
                    cur_term: 0,
                    cur_vote: None,
                };
                for line in a.iter().rev() {
                    match line {
                        Ok(ss) => {
                            has = true;
                            hs = serde_json::from_str(ss).unwrap();
                            break;
                        }
                        Err(e) => {
                            panic!("{}", e);
                        }
                    };
                }
                if has {
                    (hs.cur_term, hs.cur_vote)
                } else {
                    (0, None)
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => (0, None),
                _e => {
                    panic!("{}", err);
                }
            },
        };
        let var_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(var_path)
            .unwrap();

        let p = path.clone() + "/Log/log.txt";
        let log_path = Path::new(OsStr::new(p.as_str()));
        let log_file = OpenOptions::new().read(true).open(log_path);
        let (first_index, last_index) = match log_file {
            Ok(log_file) => {
                let reader = BufReader::new(log_file);
                let a = reader.lines().collect::<Vec<_>>();
                let first_index: usize = match a.first() {
                    Some(l) => match l {
                        Ok(ss) => {
                            let log_entry: MyLogEntry = serde_json::from_str(ss.as_str()).unwrap();
                            log_entry.index
                        }
                        Err(e) => panic!("{}", e),
                    },
                    None => 0,
                };
                let last_index: usize = match a.last() {
                    Some(l) => match l {
                        Ok(ss) => {
                            let log_entry: MyLogEntry = serde_json::from_str(ss.as_str()).unwrap();
                            log_entry.index + 1
                        }
                        Err(e) => panic!("{}", e),
                    },
                    None => 0,
                };
                (first_index, last_index)
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => (0, 0),
                _e => {
                    panic!("{}", err);
                }
            },
        };
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        println!("Store create end!");
        let re = FileStore {
            path: path,
            var_file: var_file,
            log_file: Some(log_file),
            cur_term: cur_term,
            cur_vote: cur_vote,
            first_index: first_index,
            last_index: last_index,
        };
        println!("{:?}",re);
        re
    }
}

impl Storage<StoreCommand> for FileStore {
    fn push_entry(&mut self, entry: little_raft::message::LogEntry<StoreCommand>) {
        if entry.index != self.last_index {
            panic!("Entry index error! {} {}", entry.index, self.last_index);
        }
        let myentry = MyLogEntry {
            transition: entry.transition,
            index: entry.index,
            term: entry.term,
        };
        let mut s = serde_json::to_string(&myentry).unwrap();
        s.push_str("\n");
        self.log_file.as_ref().unwrap().write_all(s.as_bytes()).unwrap();
        self.var_file.flush().unwrap();
        unistd::fsync(self.var_file.as_raw_fd()).unwrap();
        self.last_index += 1;
    }

    fn truncate_entries(&mut self, index: usize) {
        info!("Truncate {}", index);
        self.log_file = None;
        let p = self.path.clone() + "/Log/log.txt";
        let log_path = Path::new(OsStr::new(p.as_str()));
        let log_file = OpenOptions::new().read(true).open(log_path).unwrap();
        let p = self.path.clone() + "/Log/log_tmp.txt";
        let new_log_path = Path::new(OsStr::new(p.as_str()));
        let mut new_log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(new_log_path)
            .unwrap();

        let reader = BufReader::new(log_file);
        let a = reader.lines().collect::<Vec<_>>();
        for line in a.iter() {
            match line {
                Ok(ss) => {
                    let my_log_entry: MyLogEntry = serde_json::from_str(ss).unwrap();
                    if my_log_entry.index >= index
                    {
                        self.last_index = index;
                        break;
                    }
                    let mut s = ss.clone();
                    s.push_str("\n");
                    new_log_file.write_all(s.as_bytes()).unwrap();
                }
                Err(e) => {
                    panic!("{}", e);
                }
            };
        }
        drop(new_log_file);
        fs::rename(new_log_path, log_path).unwrap();
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        self.log_file = Some(log_file);
    }

    fn store_term(&mut self, term: usize) {
        self.cur_term = term;
        let hs = HardState {
            cur_term: self.cur_term,
            cur_vote: self.cur_vote,
        };
        let mut s = serde_json::to_string(&hs).unwrap();
        s.push_str("\n");
        self.var_file.write_all(s.as_bytes()).unwrap();
        self.var_file.flush().unwrap();
        unistd::fsync(self.var_file.as_raw_fd()).unwrap();
    }

    fn store_vote(&mut self, vote: Option<usize>) {
        self.cur_vote = vote;
        let hs = HardState {
            cur_term: self.cur_term,
            cur_vote: self.cur_vote,
        };
        let mut s = serde_json::to_string(&hs).unwrap();
        s.push_str("\n");
        self.var_file.write_all(s.as_bytes()).unwrap();
        self.var_file.flush().unwrap();
        unistd::fsync(self.var_file.as_raw_fd()).unwrap();
    }

    fn get_term(&self) -> usize {
        return self.cur_term;
    }

    fn get_vote(&self) -> Option<usize> {
        return self.cur_vote;
    }

    fn entries(&mut self, low: usize, high: usize) -> Vec<little_raft::message::LogEntry<StoreCommand>> {
        self.log_file = None;
        let p = self.path.clone() + "/Log/log.txt";
        let log_path = Path::new(OsStr::new(p.as_str()));
        let log_file = OpenOptions::new().read(true).open(log_path).unwrap();
        let reader = BufReader::new(log_file);
        let a = reader.lines().collect::<Vec<_>>();
        let mut re: Vec<LogEntry<StoreCommand>> = vec![];
        for line in a.iter() {
            match line {
                Ok(ss) => {
                    let my_log_entry: MyLogEntry = serde_json::from_str(ss).unwrap();
                    if my_log_entry.index >= low {
                        if my_log_entry.index >= high{
                            break;
                        }
                        re.push(LogEntry{
                            transition: my_log_entry.transition,
                            index: my_log_entry.index,
                            term: my_log_entry.term,
                        });
                    }
                }
                Err(e) => {
                    panic!("{}", e);
                }
            };
        }
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        self.log_file = Some(log_file);
        re
    }

    fn last_index(&self) -> usize {
        return self.last_index;
    }

    fn first_index(&self) -> usize {
        return self.first_index;
    }
}
