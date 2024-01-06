use rand::Rng;
use tempfile::tempfile;
use tracing::level_filters::LevelFilter;
use weaver_core::data::row::Row;
use weaver_core::data::values::Value;
use weaver_core::key::KeyData;
use weaver_core::storage::b_plus_tree::{print_structure, DiskBPlusTree};

fn insert_rand(count: usize) {
    insert(
        (0..count)
            .into_iter()
            .map(|_| rand::thread_rng().gen_range(-10_000..=10_000)),
    )
}

fn insert<I: IntoIterator<Item = i64>>(iter: I) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .try_init();
    let temp = tempfile().unwrap();
    let mut btree = DiskBPlusTree::new(temp).expect("couldn't create btree");

    let result = iter.into_iter().try_for_each(|id: i64| {
        btree.insert(
            KeyData::from([Value::from(id)]),
            Row::from([Value::from(id), Value::from(id)]).to_owned(),
        )
    });
    println!("final depth: {}", btree.depth());
    println!("final node count: {}", btree.nodes());
    println!("target depth: {}", btree.optimal_depth());
    println!("balance factors: {:#?}", btree.balance_factor());
    print_structure(&btree);
    let _ = result.expect("failed");
    assert!(btree.is_balanced(), "btree is not balanced");
}

#[test]
fn insert_100() {
    insert_rand(100);
}

#[test]
fn insert_1000() {
    insert_rand(1000);
}

#[test]
fn insert_10000() {
    insert_rand(10000);
}

#[test]
fn insert_0_to_10000() {
    insert(0..=10000);
}

#[test]
fn insert_10000_to_0() {
    insert((0..=10000).into_iter().rev());
}
