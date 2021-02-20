/*
Lacks:

1. Not allowed to updated indexed field.
2. No delete operation.
3. Impossible to search for records in index from a known range of values
    (allowed only by exact equity).
*/

#![allow(dead_code)]

use std::{fs, io};

use lbase::table::*;
use lbase::index::*;
use lbase::heap::*;
use lbase::table_trait::*;


/* Person */

#[derive(Debug, Clone, Copy)]
struct Person {
    age: u8,
    name: [u8; 4],
    about: usize,
}


impl TableTrait for Person {}


impl Person {
    fn create(
                table: &Table, index_age: &Table, heap_about: &Heap,
                age: u8, name: [u8; 4], about: &str,
            ) -> Result<(usize, Self), io::Error> {
        let mut obj = Self { age, name, about: 0 };
        obj.about = heap_about.append(about.as_bytes())?;
        let rec_id = obj.insert(table)?;
        let idx_id = Index::add(index_age, &obj.age)?;
        assert_eq!(rec_id, idx_id);
        Ok((rec_id, obj))
    }

    fn get_about(&self, heap_about: &Heap) -> Result<String, io::Error> {
        let bytes = heap_about.get(self.about)?;
        Ok(String::from_utf8(bytes).unwrap())
    }

    fn update_about(
                &mut self, heap_about: &Heap, about: &[u8]
            ) -> Result<(), io::Error> {
        self.about = heap_about.update(about, self.about)?;
        Ok(())
    }
}


/* LbaseExample */

#[derive(Debug)]
struct LbaseExample {
    person_table: Table,
    person_index_age: Table,
    person_heap_about: Heap,
}


impl LbaseExample {
    fn new(path: &str) -> Self {
        if !fs::metadata(path).is_ok() {
            fs::create_dir(path).unwrap();
        }
        Self {
            person_table: Table::new::<Person>(
                format!("{}/person.tbl", path).as_str()
            ),
            person_index_age: Table::new::<Index<u8>>(
                format!("{}/person-index-age.idx", path).as_str()
            ),
            person_heap_about: Heap::new(
                format!("{}/person-heap-about.heap", path).as_str()
            ),
        }
    }
}


fn main() {
    let db = LbaseExample::new("lbase-example");
    println!("{:?}", db);

    for (id, pers) in Person::all(&db.person_table) {
        println!(
            "{} {:?} {:?}",
            id, pers, &pers.get_about(&db.person_heap_about).unwrap()
        );
    }

    for (id, obj) in Index::<u8>::all(&db.person_index_age) {
        println!("{} {:?}", id, obj);
    }

    println!("{:?}",
        Index::search_many(&db.person_index_age, &32u8).collect::<Vec<usize>>()
    );

    // Create a person
    // let (id, alex) = Person::create(
    //     &db.person_table, &db.person_index_age, &db.person_heap_about,
    //     32, *b"alex", "Info.",
    // ).unwrap();
    // println!("{:?} {:?}", id, alex);

    // Update a person
    // let id = 2;
    // let mut obj = Person::get(&db.person_table, id).unwrap();
    // println!("{:?}", obj);
    // obj.update_about(&db.person_heap_about, b"Some relatively long about.").unwrap();
    // obj.update(&db.person_table, id).unwrap();
}
