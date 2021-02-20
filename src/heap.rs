use std::{fs, io, mem};
use std::os::unix::prelude::FileExt;
use std::convert::TryInto;


const USIZE_SIZE: usize = mem::size_of::<usize>();


#[derive(Debug)]
pub struct Heap {
    path: String,
    file: fs::File,
}


impl Heap {
    pub fn new(path: &str) -> Self {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path).unwrap();
        Self {
            path: path.to_string(),
            file,
        }
    }

    pub fn size(&self) -> usize {
        self.file.metadata().unwrap().len() as usize
    }

    pub fn empty(&self) -> bool {
        self.size() == 0
    }

    pub fn append(&self, block: &[u8]) -> Result<usize, io::Error> {
        let pos = self.size();
        let buffer_len = Self::_calc_buffer_len(block.len());
        self._write_block(block, pos, buffer_len)?;
        Ok(pos)
    }

    pub fn get(&self, pos: usize) -> Result<Vec<u8>, io::Error> {
        let block_len = self._parse_head(pos)?.0;
        let mut block: Vec<u8> = vec![0; block_len];
        self.file.read_exact_at(&mut block, (pos + 2 * USIZE_SIZE) as u64)?;
        Ok(block)
    }

    pub fn update(
                &self, block: &[u8], pos: usize
            ) -> Result<usize, io::Error> {
        let buffer_len_old = self._parse_head(pos)?.1;
        let buffer_len_new = Self::_calc_buffer_len(block.len());

        if buffer_len_new > buffer_len_old {
            let pos_end = self.size();
            self._write_block(block, pos_end, buffer_len_new)?;
            return Ok(pos_end);
        } else {
            self._write_block(block, pos, buffer_len_old)?;
            return Ok(pos);
        }
    }

    fn _parse_head(&self, pos: usize) -> Result<(usize, usize), io::Error> {
        let mut head = [0u8; 2 * USIZE_SIZE];
        self.file.read_exact_at(&mut head, pos as u64)?;
        let block_len = usize::from_le_bytes(
            head[..USIZE_SIZE].try_into().unwrap()
        );
        let buffer_len = usize::from_le_bytes(
            head[USIZE_SIZE..].try_into().unwrap()
        );
        Ok((block_len, buffer_len))
    }

    fn _write_block(
                &self, block: &[u8], pos: usize, buffer_len: usize
            ) -> Result<(), io::Error> {
        let block_len = block.len();
        let mut buffer: Vec<u8> = vec![0u8; buffer_len];
        buffer[..USIZE_SIZE].clone_from_slice(&block_len.to_le_bytes());
        buffer[USIZE_SIZE .. (2 * USIZE_SIZE)]
            .clone_from_slice(&buffer_len.to_le_bytes());
        buffer[(2 * USIZE_SIZE) .. (2 * USIZE_SIZE + block_len)]
            .clone_from_slice(block);
        self.file.write_all_at(&buffer, pos as u64)?;
        Ok(())
    }

    fn _calc_buffer_len(block_len: usize) -> usize {
        (2 * USIZE_SIZE + block_len).next_power_of_two()
    }
}


#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    const HEAP_PATH: &str = "test-heap.heap";

    #[test]
    fn test() {
        _ensure_removed_heap();

        let heap = Heap::new(HEAP_PATH);

        assert_eq!(heap.size(), 0);

        let pos = heap.append(b"Yes").unwrap();
        assert_eq!(pos, 0);
        assert_eq!(heap.size(), 32);

        let pos = heap.append(b"Info").unwrap();
        assert_eq!(pos, 32);
        assert_eq!(heap.size(), 64);

        let pos = heap.append(
            b"A long string that is longer than 32 characters."
        ).unwrap();
        assert_eq!(pos, 64);
        assert_eq!(heap.size(), 128);

        assert_eq!(heap.get(0).unwrap(), b"Yes");
        assert_eq!(heap.get(32).unwrap(), b"Info");
        assert_eq!(
            heap.get(64).unwrap(),
            b"A long string that is longer than 32 characters."
        );

        let pos = heap.update(b"info", 32).unwrap();

        assert_eq!(pos, 32);
        assert_eq!(heap.size(), 128);
        assert_eq!(heap.get(32).unwrap(), b"info");

        let pos = heap.update(
            b"Another long string that is longer than 32 characters.", 32
        ).unwrap();

        assert_eq!(pos, 128);
        assert_eq!(heap.size(), 256);
        assert_eq!(
            heap.get(128).unwrap(),
            b"Another long string that is longer than 32 characters."
        );

        let pos = heap.update(b"info", 128).unwrap();

        assert_eq!(pos, 128);
        assert_eq!(heap.size(), 256);
        assert_eq!(heap.get(128).unwrap(), b"info");

        let pos = heap.update(
            b"Another long string that is longer than 32 characters.", 128
        ).unwrap();

        assert_eq!(pos, 128);
        assert_eq!(heap.size(), 256);
        assert_eq!(
            heap.get(128).unwrap(),
            b"Another long string that is longer than 32 characters."
        );

        _ensure_removed_heap();
    }

    fn _ensure_removed_heap() {
        if fs::metadata(HEAP_PATH).is_ok() {
            fs::remove_file(HEAP_PATH).unwrap();
        }
    }
}
