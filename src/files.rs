use std::{
    fs::File,
    io::Result,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::prelude::FileExt,
};

#[derive(Debug, Clone, Copy)]
struct Index {
    start: u64,
    length: u64,
}

impl Index {
    const LENGTH: usize = 16;

    fn from_bytes(bytes: &[u8]) -> Self {
        let bytes: [u8; 16] = bytes[..16].try_into().unwrap();
        let [start, length]: [u64; 2] = unsafe { std::mem::transmute(bytes) };
        Self { start, length }
    }

    fn to_bytes(self) -> [u8; 16] {
        unsafe { std::mem::transmute([self.start, self.length]) }
    }
}

pub struct State {
    data: File,
    indices: File,
    offset: u64,
}

impl State {
    pub fn new(data_path: &str, indices_path: &str) -> Result<Self> {
        let mut options = File::options();
        options.write(true).read(true).append(true).create(true);

        let mut indices = options.open(indices_path)?;
        let offset = State::get_offset(&mut indices).unwrap_or(0);
        Ok(Self {
            data: options.open(data_path)?,
            indices,
            offset,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.indices.flush()?;
        self.data.flush()?;

        Ok(())
    }

    fn get_offset(index_file: &mut File) -> Result<u64> {
        index_file.seek(SeekFrom::End(-1 * Index::LENGTH as i64))?;

        let mut buffer = [0; Index::LENGTH];

        index_file.read_exact(&mut buffer)?;
        let index = Index::from_bytes(&buffer);

        Ok(index.start + index.length)
    }

    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let index = Index {
            start: self.offset,
            length: data.len() as u64,
        };

        self.offset += index.length;

        // Move cursor to the end
        self.data.seek(SeekFrom::End(0))?;
        self.indices.seek(SeekFrom::End(0))?;

        // Write all the data
        self.data.write_all(data)?;
        self.indices.write_all(&index.to_bytes())?;

        Ok(())
    }

    pub fn read(&mut self, index: u64) -> Result<Vec<u8>> {
        let mut buffer = [0; Index::LENGTH];

        self.indices
            .read_exact_at(&mut buffer, index * Index::LENGTH as u64)?;
        let index = Index::from_bytes(&buffer);

        let mut buffer = Vec::with_capacity(index.length as usize);
        buffer.resize(index.length as usize, 0);

        self.data.read_exact_at(&mut buffer, index.start)?;

        Ok(buffer)
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

        state.write(&get_input(0, 8)).unwrap();
        state.write(&get_input(2, 3)).unwrap();
        state.write(&get_input(5, 2)).unwrap();
        state.write(&get_input(10, 20)).unwrap();

        let ret = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        let ret = state.read(1).unwrap();
        assert_eq!(ret, get_input(2, 3));
        let ret = state.read(2).unwrap();
        assert_eq!(ret, get_input(5, 2));
        let ret = state.read(3).unwrap();
        assert_eq!(ret, get_input(10, 20));

        let ret = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
    }

    #[test]
    #[serial]
    fn test_resume() {
        clear();

        let mut state = State::new(DATA_FILE, INDEX_FILE).unwrap();

        state.write(&get_input(0, 8)).unwrap();
        state.write(&get_input(2, 3)).unwrap();
        state.write(&get_input(5, 2)).unwrap();
        state.write(&get_input(10, 20)).unwrap();

        state.flush().unwrap();

        let mut state = State::new(DATA_FILE, INDEX_FILE).unwrap();
        state.write(&get_input(4, 42)).unwrap();

        let ret = state.read(4).unwrap();
        assert_eq!(ret, get_input(4, 42));

        let ret = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
        let ret = state.read(1).unwrap();
        assert_eq!(ret, get_input(2, 3));
        let ret = state.read(2).unwrap();
        assert_eq!(ret, get_input(5, 2));
        let ret = state.read(3).unwrap();
        assert_eq!(ret, get_input(10, 20));

        let ret = state.read(0).unwrap();
        assert_eq!(ret, get_input(0, 8));
    }
}
