use std::{fs, io, iter};
use std::os::unix::prelude::FileExt;

use crate::table_trait::TableTrait;


#[derive(Debug)]
pub struct Table {
    path: String,
    block_size: usize,
    file: fs::File
}


impl Table {
    pub fn new<T: TableTrait>(path: &str) -> Self {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path).unwrap();
        Self {
            path: path.to_string(),
            block_size: T::block_size(),
            file
        }
    }

    pub fn size(&self) -> usize {
        self.file.metadata().unwrap().len() as usize / self.block_size
    }

    pub fn empty(&self) -> bool {
        self.size() == 0
    }

    pub fn get(&self, idx: usize) -> Result<Vec<u8>, io::Error> {
        let mut block: Vec<u8> = vec![0; self.block_size];
        self.file.read_exact_at(&mut block, (idx * self.block_size) as u64)?;
        Ok(block)
    }

    pub fn get_many(
                &self,
                idx_from: usize,
                idx_to: usize
            ) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut bytes: Vec<u8> =
            vec![0; self.block_size * (idx_to - idx_from)];
        self.file.read_exact_at(
            &mut bytes, (idx_from * self.block_size) as u64
        )?;
        Ok(bytes.chunks(self.block_size)
            .map(|block| block.to_vec())
            .collect()
        )
    }

    pub fn insert(&self, block: &[u8]) -> Result<usize, io::Error> {
        let idx = self.size();
        self.file.write_all_at(block, (idx * self.block_size) as u64)?;
        Ok(idx)
    }

    pub fn update(
                &self,
                block: &[u8],
                idx: usize
            ) -> Result<(), io::Error> {
        self.file.write_all_at(block, (idx * self.block_size) as u64)?;
        Ok(())
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (usize, Vec<u8>)> + '_> {
        self.iter_between(0, self.size()).unwrap()
    }

    pub fn iter_between(
                &self,
                idx_from: usize,
                idx_to: usize
            ) -> Result<
                Box<dyn Iterator<Item = (usize, Vec<u8>)> + '_>,
                io::Error
            > {
        let mut idx = idx_from;

        Ok(Box::new(iter::from_fn(move || {
            let result;
            if idx < idx_to {
                let block = self.get(idx).unwrap();
                result = Some((idx, block));
                idx += 1;
            } else {
                result = None;
            }
            result
        })))
    }
}
