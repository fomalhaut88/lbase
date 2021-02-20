use std::{io, iter};

use crate::table_trait::TableTrait;
use crate::table::Table;


#[derive(Debug, Copy, Clone)]
pub struct Index<T> {
    left: usize,
    right: usize,
    value: T,
}


impl<T: Copy> TableTrait for Index<T> {}


impl<'a, T: 'a + Copy + Clone + PartialOrd> Index<T> {
    fn new(value: &T) -> Self {
        Self {
            left: 0,
            right: 0,
            value: value.clone(),
        }
    }

    pub fn add(
                table: &Table,
                value: &T
            ) -> Result<usize, io::Error> {
        let record = Self::new(value);
        let record_id = record.insert(table)?;
        Self::_bind(table, value, record_id)?;
        Ok(record_id)
    }

    // TODO: fn add_many

    pub fn search_one(
                table: &Table,
                value: &T
            ) -> Result<usize, io::Error> {
        match Self::search_many(table, value).nth(0) {
            Some(id) => Ok(id),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "table index"))
        }
    }

    pub fn search_many(
                table: &'a Table,
                value: &'a T
            ) -> Box<dyn Iterator<Item = usize> + 'a> {
        let mut id = if table.empty() { 0 } else { 1 };

        Box::new(iter::from_fn(move || {
            let mut result = None;

            while id > 0 {
                let rec = Self::get(table, id).unwrap();

                if *value < rec.value {
                    id = rec.left;

                } else {
                    if *value == rec.value {
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

    fn _bind(
                table: &Table, value: &T, record_id: usize
            ) -> Result<(), io::Error> {
        let mut id = 1;
        let mut id_next;

        if id != record_id {
            while id > 0 {
                let mut rec = Self::get(table, id)?;

                if *value < rec.value {
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

        let index = Table::new::<Index<u8>>(INDEX_PATH);

        Index::<u8>::add(&index, &32).unwrap();
        Index::<u8>::add(&index, &33).unwrap();
        Index::<u8>::add(&index, &12).unwrap();
        Index::<u8>::add(&index, &90).unwrap();
        Index::<u8>::add(&index, &32).unwrap();

        assert_eq!(index.size(), 5);

        let record = Index::<u8>::get(&index, 1).unwrap();
        assert_eq!(record.left, 3);
        assert_eq!(record.right, 2);
        assert_eq!(record.value, 32);

        let record = Index::<u8>::get(&index, 2).unwrap();
        assert_eq!(record.left, 5);
        assert_eq!(record.right, 4);
        assert_eq!(record.value, 33);

        let record = Index::<u8>::get(&index, 3).unwrap();
        assert_eq!(record.left, 0);
        assert_eq!(record.right, 0);
        assert_eq!(record.value, 12);

        let record = Index::<u8>::get(&index, 4).unwrap();
        assert_eq!(record.left, 0);
        assert_eq!(record.right, 0);
        assert_eq!(record.value, 90);

        let record = Index::<u8>::get(&index, 5).unwrap();
        assert_eq!(record.left, 0);
        assert_eq!(record.right, 0);
        assert_eq!(record.value, 32);

        assert_eq!(Index::<u8>::search_one(&index, &90).unwrap(), 4);

        assert_eq!(
            Index::<u8>::search_many(&index, &32).collect::<Vec<usize>>(),
            vec![1, 5]
        );

        assert_eq!(
            Index::<u8>::search_many(&index, &33).collect::<Vec<usize>>(),
            vec![2]
        );

        assert_eq!(
            Index::<u8>::search_many(&index, &35).collect::<Vec<usize>>(),
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
