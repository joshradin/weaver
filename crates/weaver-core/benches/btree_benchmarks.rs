use std::iter;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput, BatchSize};
use rand::distributions::Alphanumeric;
use rand::prelude::ThreadRng;
use rand::{Rng, thread_rng};
use tempfile::tempfile;
use weaver_core::data::row::Row;
use weaver_core::data::values::Literal;
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

fn insert_rand_with<V: Into<Literal>, F: Fn(&mut ThreadRng) -> V>(count: usize, page_len: usize, prod: F) -> BPlusTree<PagedVec> {
    let mut rng = rand::thread_rng();
    insert((0..count).into_iter().map(|_| prod(&mut rng)), page_len)
}

fn insert<V : Into<Literal>, I: IntoIterator<Item = V>>(iter: I, page_len: usize) -> BPlusTree<PagedVec> {
    let mut btree = BPlusTree::new(PagedVec::new(page_len));

    iter.into_iter()
        .try_for_each(|v: V| {
            let v = v.into();
            btree.insert(
                KeyData::from([v.clone()]),
                Row::from([v.clone(), v.clone()]).to_owned(),
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

fn btree_insert_rand_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("random string elements");
    group.sample_size(10);
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter_batched(|| {
                iter::repeat_with(|| {
                    thread_rng().sample_iter(&Alphanumeric)
                       .take(rand::thread_rng().gen_range(5..=15))
                       .map(char::from)
                       .collect::<String>()
                })
                    .take(count as usize)
            } , |iter| insert(iter, 4096), BatchSize::SmallInput);
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
    group.bench_function("random access", |b| {
        b.iter(|| {
            let ref id = KeyData::from([rand::thread_rng().gen_range(0..10000)]);
            let _ = btree.get(id);
        });
    });
}

criterion_group!(
    insertion,
    btree_insert_rand,
    btree_insert_rand_strings,
    btree_insert_inc,
    btree_insert_dec
);
criterion_group!(read, btree_read);
criterion_main!(insertion, read);
