use std::{mem, slice, io};

use crate::table::Table;


pub trait TableTrait where Self: Sized + Copy {
    fn block_size() -> usize {
        mem::size_of::<Self>()
    }

    fn as_bytes(&self) -> &[u8] {
        let pointer = (self as *const Self) as *const u8;
        unsafe {
            slice::from_raw_parts(pointer, Self::block_size())
        }
    }

    fn from_bytes(block: &[u8]) -> Self {
        let pointer = (block as *const [u8]) as *const Self;
        unsafe {
            slice::from_raw_parts(pointer, Self::block_size())[0]
        }
    }

    fn get(table: &Table, id: usize) -> Result<Self, io::Error> {
        let block = table.get(id - 1)?;
        Ok(Self::from_bytes(&block))
    }

    fn get_many(
                table: &Table, id_from: usize, id_to: usize
            ) -> Result<Vec<Self>, io::Error> {
        Ok(
            table.get_many(id_from - 1, id_to - 1)?
                .iter()
                .map(|block| Self::from_bytes(&block))
                .collect()
        )
    }

    fn insert(&self, table: &Table) -> Result<usize, io::Error> {
        Ok(table.insert(&self.as_bytes())? + 1)
    }

    fn update(&self, table: &Table, id: usize) -> Result<(), io::Error> {
        table.update(&self.as_bytes(), id - 1)?;
        Ok(())
    }

    fn all(table: &Table) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        Box::new(table.iter().map(
            |(idx, block)| (idx + 1, Self::from_bytes(&block))
        ))
    }
}


#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    const TABLE_PATH: &str = "test-person.idx";

    #[derive(Debug, Clone, Copy, PartialEq)]
    struct Person {
        age: u8,
        name: [u8; 4],
    }

    impl TableTrait for Person {}

    #[test]
    fn test() {
        _ensure_removed_table();

        assert_eq!(Person::block_size(), 5);

        let table = Table::new::<Person>(TABLE_PATH);

        let person = Person { age: 32, name: *b"alex" };
        let id = person.insert(&table).unwrap();
        assert_eq!(id, 1);
        assert_eq!(table.size(), 1);

        let person = Person { age: 33, name: *b"Alex" };
        let id = person.insert(&table).unwrap();
        assert_eq!(id, 2);
        assert_eq!(table.size(), 2);

        assert_eq!(
            Person::get(&table, 1).unwrap(),
            Person { age: 32, name: *b"alex" }
        );
        assert_eq!(
            Person::get(&table, 2).unwrap(),
            Person { age: 33, name: *b"Alex" }
        );
        assert_eq!(
            Person::all(&table).collect::<Vec<(usize, Person)>>(),
            vec![
                (1, Person { age: 32, name: *b"alex" }),
                (2, Person { age: 33, name: *b"Alex" })
            ]
        );

        let id = 2;
        let mut person = Person::get(&table, id).unwrap();
        person.age = 35;
        person.update(&table, id).unwrap();
        assert_eq!(
            Person::get(&table, id).unwrap(),
            Person { age: 35, name: *b"Alex" }
        );

        _ensure_removed_table();
    }

    fn _ensure_removed_table() {
        if fs::metadata(TABLE_PATH).is_ok() {
            fs::remove_file(TABLE_PATH).unwrap();
        }
    }
}
