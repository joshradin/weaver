use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use tempfile::tempfile;
use weaver_core::data::row::Row;
use weaver_core::data::values::Value;
use weaver_core::key::KeyData;
use weaver_core::storage::b_plus_tree::BPlusTree;
use weaver_core::storage::PagedVec;

fn insert_rand(count: usize, page_len: usize) -> BPlusTree<PagedVec> {
    insert(
        (0..count)
            .into_iter()
            .map(|_| rand::thread_rng().gen_range(0..count as i64)),
        page_len,
    )
}

fn insert<I: IntoIterator<Item = i64>>(iter: I, page_len: usize) -> BPlusTree<PagedVec> {
    let mut btree = BPlusTree::new(PagedVec::new(page_len));

    iter.into_iter()
        .try_for_each(|id: i64| {
            btree.insert(
                KeyData::from([Value::from(id)]),
                Row::from([Value::from(id), Value::from(id)]).to_owned(),
            )
        })
        .unwrap();
    btree
}

fn insert_0_to_10000() {
    insert(0..=10000, 4096);
}

fn insert_10000_to_0() {
    insert((0..=10000).into_iter().rev(), 4096);
}

fn btree_insert_rand(c: &mut Criterion) {
    let mut group = c.benchmark_group("random elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert_rand(count as usize, 4096));
        });
    }
}

fn btree_insert_inc(c: &mut Criterion) {
    let mut group = c.benchmark_group("increasing elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert(0..count as i64, 4096));
        });
    }
}

fn btree_insert_dec(c: &mut Criterion) {
    let mut group = c.benchmark_group("decreasing elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert((0..count as i64).rev(), 4096));
        });
    }
}

fn btree_read(c: &mut Criterion) {
    let mut btree = insert_rand(10000, 4096 * 4);
    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("read", |b| {
        b.iter(|| {
            let ref id = KeyData::from([rand::thread_rng().gen_range(0..10000)]);
            let _ = btree.get(id);
        });
    });
}

criterion_group!(
    insertion,
    btree_insert_rand,
    btree_insert_inc,
    btree_insert_dec
);
criterion_group!(read, btree_read);
criterion_main!(insertion, read);
