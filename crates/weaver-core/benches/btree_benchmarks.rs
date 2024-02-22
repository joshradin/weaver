use std::iter;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main, Throughput};
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use rand::prelude::ThreadRng;

use weaver_core::data::row::Row;
use weaver_core::data::values::DbVal;
use weaver_core::key::KeyData;
use weaver_core::monitoring::{Monitorable, MonitorCollector};
use weaver_core::storage::b_plus_tree::BPlusTree;
use weaver_core::storage::VecPager;

fn insert_rand<'a>(
    count: usize,
    page_len: usize,
    monitor_collector: impl Into<Option<&'a mut MonitorCollector>>,
) -> BPlusTree<VecPager> {
    insert(
        (0..count)
            .into_iter()
            .map(|_| rand::thread_rng().gen_range(0..count as i64)),
        page_len,
        monitor_collector
    )
}

fn insert_rand_with<'a, V: Into<DbVal>, F: Fn(&mut ThreadRng) -> V>(
    count: usize,
    page_len: usize,
    prod: F,
    monitor_collector: impl Into<Option<&'a mut MonitorCollector>>,
) -> BPlusTree<VecPager> {
    let mut rng = rand::thread_rng();
    insert((0..count).into_iter().map(|_| prod(&mut rng)), page_len, monitor_collector)
}

fn insert<'a, V: Into<DbVal>, I: IntoIterator<Item = V>>(
    iter: I,
    page_len: usize,
    monitor_collector: impl Into<Option<&'a mut MonitorCollector>>,
) -> BPlusTree<VecPager> {
    let mut btree = BPlusTree::new(VecPager::new(page_len));
    if let Some(monitor_collector) = monitor_collector.into() {
        monitor_collector.push_monitorable(&btree);
    }

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

fn btree_insert_rand(c: &mut Criterion) {
    let mut group = c.benchmark_group("random elements");
    group.sample_size(10);
    let mut mc = MonitorCollector::new();
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert_rand(count as usize, 4096, &mut mc));
        });
    }
    println!("{:#?}", mc.all());
}

fn btree_insert_rand_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("random string elements");
    group.sample_size(10);
    let mut mc = MonitorCollector::new();
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter_batched(
                || {
                    iter::repeat_with(|| {
                        thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(rand::thread_rng().gen_range(5..=15))
                            .map(char::from)
                            .collect::<String>()
                    })
                    .take(count as usize)
                },
                |iter| insert(iter, 4096, &mut mc),
                BatchSize::SmallInput,
            );
        });
    }
    println!("{:#?}", mc.all());
}

fn btree_insert_inc(c: &mut Criterion) {
    let mut group = c.benchmark_group("increasing elements");
    group.sample_size(10);
    let mut mc = MonitorCollector::new();
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert(0..count as i64, 4096, &mut mc));
        });
    }
    println!("{:#?}", mc.all());
}

fn btree_insert_dec(c: &mut Criterion) {
    let mut group = c.benchmark_group("decreasing elements");
    group.sample_size(10);
    let mut mc = MonitorCollector::new();
    for count in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(*count));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| insert((0..count as i64).rev(), 4096, &mut mc));
        });
    }
    println!("{:#?}", mc.all());
}

fn btree_read(c: &mut Criterion) {
    let mut btree = insert_rand(10000, 4096 * 4, None);
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
