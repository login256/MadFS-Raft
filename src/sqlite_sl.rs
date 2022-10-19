use std::{ffi::CString, path::PathBuf};

use libc;
use sqlite::Connection;
use sqlite3_sys;

extern "C" {
    fn loadOrSaveDb(
        pInMemory: *mut sqlite3_sys::sqlite3,
        zFilename: *const libc::c_char,
        isSave: libc::c_int,
    ) -> libc::c_int;
}

pub fn load_or_save(
    connection: &Connection,
    file_name: &PathBuf,
    is_save: bool,
) -> Result<(), sqlite::Error> {
    let is_save = if is_save { 1 } else { 0 };
    let file_name = CString::new(file_name.as_os_str().to_str().unwrap()).unwrap();
    let re = unsafe { loadOrSaveDb(connection.as_raw(), file_name.as_ptr(), is_save) };
    if re != 0 {
        return Err(sqlite::Error {
            code: Some(re as isize),
            message: Some("Save or Load error!".to_string()),
        });
    }
    Ok(())
}
