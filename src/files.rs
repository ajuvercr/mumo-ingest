use std::{
    fs::File,
    io::Result,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::prelude::FileExt,
    path::Path,
};

use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct Index {
    start: u64,
    pub index: u64,
    length: u64,
}

impl Index {
    const LENGTH: usize = 24;

    fn from_bytes(bytes: &[u8]) -> Self {
        let bytes: [u8; 24] = bytes[..24].try_into().unwrap();
        let [start, index, length]: [u64; 3] = unsafe { std::mem::transmute(bytes) };
        Self {
            start,
            index,
            length,
        }
    }

    fn to_bytes(self) -> [u8; 24] {
        unsafe { std::mem::transmute([self.start, self.index, self.length]) }
    }
}

#[derive(Serialize)]
pub struct Written {
    size: usize,
    index: u64,
}

pub struct State {
    data: File,
    indices: File,
    offset: u64,
    index: u64,
}

impl State {
    pub fn new(data_path: impl AsRef<Path>, indices_path: impl AsRef<Path>) -> Result<Self> {
        let mut options = File::options();
        options.write(true).read(true).append(true).create(true);

        let mut indices = options.open(indices_path)?;
        let (index, offset) = if let Ok(last_index) = State::get_last_index(&mut indices) {
            (last_index.index + 1, last_index.start + last_index.length)
        } else {
            (0, 0)
        };
        Ok(Self {
            data: options.open(data_path)?,
            indices,
            offset,
            index,
        })
    }

    pub fn last(&self) -> u64 {
        self.index
    }

    pub fn flush(&mut self) -> Result<()> {
        self.indices.flush()?;
        self.data.flush()?;

        Ok(())
    }

    fn get_last_index(index_file: &mut File) -> Result<Index> {
        index_file.seek(SeekFrom::End(-1 * Index::LENGTH as i64))?;

        let mut buffer = [0; Index::LENGTH];

        index_file.read_exact(&mut buffer)?;
        let index = Index::from_bytes(&buffer);

        Ok(index)
    }

    pub fn write(&mut self, data: &[u8], flush: bool) -> Result<Written> {
        let index = Index {
            start: self.offset,
            index: self.index,
            length: data.len() as u64,
        };

        trace!("Writing data to {:?}", index);

        self.offset += index.length;
        self.index += 1;

        // Move cursor to the end
        self.data.seek(SeekFrom::End(0))?;
        self.indices.seek(SeekFrom::End(0))?;

        // Write all the data
        self.data.write_all(data)?;
        self.indices.write_all(&index.to_bytes())?;

        if flush {
            self.flush()?;
        }

        Ok(Written {
            size: data.len(),
            index: self.index - 1,
        })
    }

    pub fn read(&mut self, idx: u64) -> Result<(Vec<u8>, Index)> {
        let mut buffer = [0; Index::LENGTH];

        self.indices
            .read_exact_at(&mut buffer, idx * Index::LENGTH as u64)?;
        let index = Index::from_bytes(&buffer);

        trace!("Reading index at {} {:?}", idx, index);

        let mut buffer = Vec::with_capacity(index.length as usize);
        buffer.resize(index.length as usize, 0);

        self.data.read_exact_at(&mut buffer, index.start)?;

        Ok((buffer, index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    const DATA_FILE: &'static str = "data.bin";
    const INDEX_FILE: &'static str = "indices.bin";

    fn clear() {
        let _ = std::fs::remove_file(DATA_FILE);
        let _ = std::fs::remove_file(INDEX_FILE);
    }

    fn get_input(index: u8, length: u8) -> Vec<u8> {
        (index..index + length).collect()
    }

    #[test]
    #[serial]
    fn basic_write() {
        clear();
        let mut state = State::new(DATA_FILE, INDEX_FILE).unwrap();

        state.write(&get_input(0, 8), false).unwrap();
        state.write(&get_input(2, 3), false).unwrap();
        state.write(&get_input(5, 2), false).unwrap();
        state.write(&get_input(10, 20), false).unwrap();

        let (ret, idx) = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        assert_eq!(idx.index, 0);
        let (ret, idx) = state.read(1).unwrap();
        assert_eq!(ret, get_input(2, 3));
        assert_eq!(idx.index, 1);
        let (ret, idx) = state.read(2).unwrap();
        assert_eq!(ret, get_input(5, 2));
        assert_eq!(idx.index, 2);
        let (ret, idx) = state.read(3).unwrap();
        assert_eq!(ret, get_input(10, 20));
        assert_eq!(idx.index, 3);

        let (ret, idx) = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        assert_eq!(idx.index, 0);
    }

    #[test]
    #[serial]
    fn test_resume() {
        clear();

        let mut state = State::new(DATA_FILE, INDEX_FILE).unwrap();

        state.write(&get_input(0, 8), false).unwrap();
        state.write(&get_input(2, 3), false).unwrap();
        state.write(&get_input(5, 2), false).unwrap();
        state.write(&get_input(10, 20), false).unwrap();

        state.flush().unwrap();

        let mut state = State::new(DATA_FILE, INDEX_FILE).unwrap();
        state.write(&get_input(4, 42), false).unwrap();

        let (ret, idx) = state.read(4).unwrap();
        assert_eq!(ret, get_input(4, 42));
        assert_eq!(idx.index, 4);

        let (ret, idx) = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        assert_eq!(idx.index, 0);
        let (ret, idx) = state.read(1).unwrap();
        assert_eq!(ret, get_input(2, 3));
        assert_eq!(idx.index, 1);
        let (ret, idx) = state.read(2).unwrap();
        assert_eq!(ret, get_input(5, 2));
        assert_eq!(idx.index, 2);
        let (ret, idx) = state.read(3).unwrap();
        assert_eq!(ret, get_input(10, 20));
        assert_eq!(idx.index, 3);

        let (ret, idx) = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        assert_eq!(idx.index, 0);
    }
}
