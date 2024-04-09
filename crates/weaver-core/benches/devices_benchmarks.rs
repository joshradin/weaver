use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use rand::{random, Rng};

use weaver_core::storage::devices::mmap_file::MMapFile;
use weaver_core::storage::devices::ram_file::RandomAccessFile;
use weaver_core::storage::devices::StorageDevice;

const IOPS: usize = 512;
const STORAGE_DEVICE_LEN: u64 = 4096;

fn init_device<T: StorageDevice + ?Sized>(device: &mut T) {
    device
        .set_len(STORAGE_DEVICE_LEN)
        .expect("could not set length");
}
fn read_device<T: StorageDevice + ?Sized>(device: &mut T) {
    for _ in 0..IOPS {
        let at = rand::thread_rng().gen_range(0..STORAGE_DEVICE_LEN);
        let mut buffer = [0_u8; 8];
        device
            .read(at, &mut buffer)
            .expect("could not read at most 8 bytes.");
    }
}

fn write_device<T: StorageDevice + ?Sized>(device: &mut T) {
    for _ in 0..IOPS {
        let at = rand::thread_rng().gen_range(0..(STORAGE_DEVICE_LEN - 8));
        let buffer = random::<u64>().to_be_bytes();
        device
            .write(at, &buffer)
            .expect("could not write at most 8 bytes.");
    }
}
fn write_device_then_flush<T: StorageDevice + ?Sized>(device: &mut T) {
    for _ in 0..IOPS {
        let at = rand::thread_rng().gen_range(0..(STORAGE_DEVICE_LEN - 8));
        let buffer = random::<u64>().to_be_bytes();
        device
            .write(at, &buffer)
            .expect("could not write at most 8 bytes.");
    }
    device.flush().expect("could not flush")
}

fn mixed_device<T: StorageDevice + ?Sized>(device: &mut T) {
    for _ in 0..IOPS {
        let at = rand::thread_rng().gen_range(0..(STORAGE_DEVICE_LEN - 8));
        if rand::random::<bool>() {
            let buffer = random::<u64>().to_be_bytes();
            device
                .write(at, &buffer)
                .expect("could not write at most 8 bytes.");
        } else {
            let mut buffer = [0_u8; 8];
            device
                .read(at, &mut buffer)
                .expect("could not read at most 8 bytes.");
        }
    }
}
fn bench_device<T: StorageDevice + ?Sized>(device: &mut T, group: &mut BenchmarkGroup<WallTime>) {
    group.throughput(Throughput::Bytes(IOPS as u64 * 8u64));
    group.bench_function("read throughput", |b| b.iter(|| read_device(device)));
    group.bench_function("write throughput", |b| b.iter(|| write_device(device)));
    group.bench_function("write-flush throughput", |b| {
        b.iter(|| write_device_then_flush(device))
    });
    group.bench_function("read+write throughput", |b| b.iter(|| mixed_device(device)));
}

fn ram_file(criterion: &mut Criterion) {
    let temp = tempfile::tempfile().expect("could not make temp file");
    let mut ram = RandomAccessFile::with_file(temp).expect("could not make ram file");
    init_device(&mut ram);
    let mut group = criterion.benchmark_group("ram_file");
    group.sample_size(1000);
    bench_device(&mut ram, &mut group);
}

fn mmap_file(criterion: &mut Criterion) {
    let temp = tempfile::tempfile().expect("could not make temp file");
    let mut mmap = MMapFile::with_file(temp).expect("could not make ram file");
    init_device(&mut mmap);
    let mut group = criterion.benchmark_group("mmap file");
    group.sample_size(1000);
    bench_device(&mut mmap, &mut group);
}

criterion_group!(benches, ram_file, mmap_file);
criterion_main!(benches);
