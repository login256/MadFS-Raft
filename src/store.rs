use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::prelude::AsRawFd,
    path::Path,
};

use crate::server::StoreCommand;
use little_raft::state_machine::HardState;
use little_raft::state_machine::Storage;
use nix::unistd;
use log::info;

//FileStore
#[derive(Debug)]
pub struct FileStore {
    path: String,
    var_file: File,
    cur_term: usize,
    cur_vote: Option<usize>,
}

impl FileStore {
    //new a FileStore
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
                        Err(_) => {}
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
        println!("Store create end!");
        FileStore {
            path: path,
            var_file: var_file,
            cur_term: cur_term,
            cur_vote: cur_vote,
        }
    }
}

impl Storage<StoreCommand> for FileStore {
    fn push_entry(&mut self, entry: little_raft::message::LogEntry<StoreCommand>) {
        todo!()
    }

    fn truncate_entries(&mut self, index: usize) {
        todo!()
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

    fn entries(&self, low: u64, high: u64) -> Vec<little_raft::message::LogEntry<StoreCommand>> {
        todo!()
    }

    fn last_index(&self) -> usize {
        todo!()
    }

    fn first_index(&self) -> usize {
        todo!()
    }
}
