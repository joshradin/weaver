use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;
use rand::Rng;

use tracing::level_filters::LevelFilter;
use weaver_core::data::row::Row;
use weaver_core::data::values::DbVal;
use weaver_core::key::KeyData;
use weaver_core::storage::b_plus_tree::BPlusTree;
use weaver_core::storage::VecPager;

fn insert_rand(count: usize) {
    insert((0..count).map(|_| rand::thread_rng().gen_range(-10_000..=10_000)))
}

fn insert_rand_with<V: Into<DbVal>, F: Fn(&mut ThreadRng) -> V>(count: usize, prod: F) {
    let mut rng = rand::thread_rng();
    insert((0..count).map(|_| prod(&mut rng)))
}

fn insert<V: Into<DbVal>, I: IntoIterator<Item = V>>(iter: I) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_thread_ids(true)
        .with_thread_names(true)
        .try_init();

    let temp = VecPager::new(4096);
    let btree = BPlusTree::new(temp);

    let mut keys = vec![];
    let result = iter.into_iter().try_for_each(|id: V| {
        let id = id.into();
        keys.push(id.clone());
        btree.insert(
            KeyData::from([id.clone()]),
            Row::from([id.clone(), id]).to_owned(),
        )
    });

    btree.print().expect("could not print");
    result.expect("failed");
    for key in keys {
        println!("checking for existence of {}", key);
        if btree.get(&KeyData::from([key.clone()])).is_err() {
            panic!("does not contain inserted value will key {}", key);
        }
    }
}

#[test]
fn insert_100() {
    insert_rand(100);
}

#[test]
fn insert_100_strings() {
    insert_rand_with(100, |rng| {
        DbVal::string(
            rng.sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(5..=15))
                .map(char::from)
                .collect::<String>(),
            16,
        )
    });
}

#[test]
fn insert_1000() {
    insert_rand(1000);
}

#[test]
fn insert_1000_strings() {
    insert_rand_with(1000, |rng| {
        DbVal::string(
            rng.sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(5..=15))
                .map(char::from)
                .collect::<String>(),
            16,
        )
    });
}

#[test]
fn insert_10000() {
    insert_rand(10000);
}

#[test]
fn insert_10000_strings() {
    insert_rand_with(10_000, |rng| {
        DbVal::string(
            rng.sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(5..=15))
                .map(char::from)
                .collect::<String>(),
            16,
        )
    });
}

#[test]
fn insert_0_to_10000() {
    insert(0..=10000);
}

#[test]
fn insert_10000_to_0() {
    insert((0..=10000).rev());
}
