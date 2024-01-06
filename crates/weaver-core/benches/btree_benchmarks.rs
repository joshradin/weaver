use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use tempfile::tempfile;
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
    let temp = tempfile().unwrap();
    let mut btree = DiskBPlusTree::new(temp).expect("couldn't create btree");

    let result = iter.into_iter().try_for_each(|id: i64| {
        btree.insert(
            KeyData::from([Value::from(id)]),
            Row::from([Value::from(id), Value::from(id)]).to_owned(),
        )
    });
    // println!("final depth: {}", btree.depth());
    // println!("final node count: {}", btree.nodes());
    // println!("target depth: {}", btree.optimal_depth());
    // println!("balance factors: {:#?}", btree.balance_factor());
    // print_structure(&btree);
    // let _ = result.expect("failed");
    // assert!(btree.is_balanced(), "btree is not balanced");
}

fn insert_0_to_10000() {
    insert(0..=10000);
}

fn insert_10000_to_0() {
    insert((0..=10000).into_iter().rev());
}

fn btree_insert_rand(c: &mut Criterion) {
    let mut group = c.benchmark_group("random elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert_rand(count as usize));
        });
    }
}

fn btree_insert_inc(c: &mut Criterion) {
    let mut group = c.benchmark_group("increasing elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert(0..count as i64));
        });
    }
}

fn btree_insert_dec(c: &mut Criterion) {
    let mut group = c.benchmark_group("decreasing elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert((0..count as i64).rev()));
        });
    }
}

criterion_group!(
    insertion,
    btree_insert_rand,
    btree_insert_inc,
    btree_insert_dec
);
criterion_main!(insertion);
