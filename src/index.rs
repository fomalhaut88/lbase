use std::{io, iter};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::table_trait::TableTrait;
use crate::table::Table;


#[derive(Debug, Copy, Clone)]
pub struct Index {
    left: usize,
    right: usize,
    hash: u64,
}


impl TableTrait for Index {}


impl<'a> Index {
    fn new(hash: u64) -> Self {
        Self {
            left: 0,
            right: 0,
            hash: hash,
        }
    }

    pub fn add<T: Hash>(
                table: &Table,
                value: &T
            ) -> Result<usize, io::Error> {
        let hash = Self::_hash_value(value);
        let record = Self::new(hash);
        let record_id = record.insert(table)?;
        Self::_bind(table, hash, record_id)?;
        Ok(record_id)
    }

    // TODO: fn add_many

    pub fn search_one<T: Hash>(
                table: &Table,
                value: &T
            ) -> Result<usize, io::Error> {
        match Self::search_many(table, value).nth(0) {
            Some(id) => Ok(id),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "table index"))
        }
    }

    pub fn search_many<T: Hash>(
                table: &'a Table,
                value: &'a T
            ) -> Box<dyn Iterator<Item = usize> + 'a> {
        let hash = Self::_hash_value(value);
        let mut id = if table.empty() { 0 } else { 1 };

        Box::new(iter::from_fn(move || {
            let mut result = None;

            while id > 0 {
                let rec = Self::get(table, id).unwrap();

                if hash < rec.hash {
                    id = rec.left;

                } else {
                    if hash == rec.hash {
                        result = Some(id);
                    }

                    id = rec.right;
                }

                if result.is_some() {
                    break;
                }
            }
            return result;
        }))
    }

    fn _hash_value<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn _bind(
                table: &Table, hash: u64, record_id: usize
            ) -> Result<(), io::Error> {
        let mut id = 1;
        let mut id_next;

        if id != record_id {
            while id > 0 {
                let mut rec = Self::get(table, id)?;

                if hash < rec.hash {
                    id_next = rec.left;
                    if id_next == 0 {
                        rec.left = record_id;
                    }
                } else {
                    id_next = rec.right;
                    if id_next == 0 {
                        rec.right = record_id;
                    }
                }

                if id_next == 0 {
                    rec.update(table, id)?;
                }

                id = id_next;
            }
        }

        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    const INDEX_PATH: &str = "test-index.idx";

    #[test]
    fn test() {
        _ensure_removed_index();

        let index = Table::new::<Index>(INDEX_PATH);

        Index::add(&index, &32).unwrap();
        Index::add(&index, &33).unwrap();
        Index::add(&index, &12).unwrap();
        Index::add(&index, &90).unwrap();
        Index::add(&index, &32).unwrap();

        assert_eq!(index.size(), 5);

        let record = Index::get(&index, 1).unwrap();
        assert_eq!(record.left, 2);
        assert_eq!(record.right, 3);
        assert_eq!(record.hash, 12435244753979506696);

        let record = Index::get(&index, 2).unwrap();
        assert_eq!(record.left, 4);
        assert_eq!(record.right, 0);
        assert_eq!(record.hash, 5455878505379436227);

        let record = Index::get(&index, 3).unwrap();
        assert_eq!(record.left, 5);
        assert_eq!(record.right, 0);
        assert_eq!(record.hash, 17877610526930097705);

        let record = Index::get(&index, 4).unwrap();
        assert_eq!(record.left, 0);
        assert_eq!(record.right, 0);
        assert_eq!(record.hash, 4907824628803523476);

        let record = Index::get(&index, 5).unwrap();
        assert_eq!(record.left, 0);
        assert_eq!(record.right, 0);
        assert_eq!(record.hash, 12435244753979506696);

        assert_eq!(Index::search_one(&index, &90).unwrap(), 4);

        assert_eq!(
            Index::search_many(&index, &32).collect::<Vec<usize>>(),
            vec![1, 5]
        );

        assert_eq!(
            Index::search_many(&index, &33).collect::<Vec<usize>>(),
            vec![2]
        );

        assert_eq!(
            Index::search_many(&index, &35).collect::<Vec<usize>>(),
            vec![]
        );

        _ensure_removed_index();
    }

    fn _ensure_removed_index() {
        if fs::metadata(INDEX_PATH).is_ok() {
            fs::remove_file(INDEX_PATH).unwrap();
        }
    }
}
