//! Provides consistent hashing via a given seed value

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{BuildHasher, Hasher};

use md5::Context;

pub type ConsistentHashMap<K, V> = HashMap<K, V, SeededHasherBuilder>;

const DEFAULT_SEED: u64 = u64::from_be_bytes(*b"_WEAVER_");

/// A seeded hash builder using a given seed value
#[derive(Debug)]
pub struct SeededHasherBuilder {
    seed: u64,
}

impl Default for SeededHasherBuilder {
    fn default() -> Self {
        Self::with_seed(DEFAULT_SEED)
    }
}

impl SeededHasherBuilder {
    /// Creates a new seeded hash builder with a given seed
    pub fn with_seed(seed: u64) -> Self {
        Self { seed }
    }
}

impl BuildHasher for SeededHasherBuilder {
    type Hasher = SeededHasher;

    fn build_hasher(&self) -> Self::Hasher {
        let mut context = Context::new();
        context.consume(self.seed.to_be_bytes());
        SeededHasher {
            seed: self.seed,
            context,
        }
    }
}

/// A hasher that hashes values using a given seed
pub struct SeededHasher {
    seed: u64,
    context: Context,
}

impl SeededHasher {
    /// Gets the seed value
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl Debug for SeededHasher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeededHasher")
            .field("seed", &self.seed)
            .finish()
    }
}

impl Hasher for SeededHasher {
    fn finish(&self) -> u64 {
        let finished: [u8; 16] = self.context.clone().compute().0;
        u64::from_be_bytes(finished[8..16].try_into().unwrap())
    }

    fn write(&mut self, bytes: &[u8]) {
        self.context.consume(bytes);
    }
}

#[cfg(test)]
mod tests {
    use crate::common::consistent_hasher::{ConsistentHashMap, SeededHasherBuilder};
    use std::hash::BuildHasher;

    #[test]
    fn consistent_hashmap() {
        let mut consistent_hash_map = ConsistentHashMap::<&str, usize>::default();
        consistent_hash_map.insert("hello, world!", 15);
        consistent_hash_map.insert("goodbye, world!", 19);
        assert_eq!(consistent_hash_map["hello, world!"], 15);
        assert_eq!(consistent_hash_map["goodbye, world!"], 19);
    }

    #[test]
    fn hash_string() {
        let seeded_hasher_builder = SeededHasherBuilder::with_seed(0xbeef);
        let h1 = seeded_hasher_builder.hash_one("Hello, World");
        let h2 = seeded_hasher_builder.hash_one("Hello, World");
        assert_eq!(h1, h2);
        let h3 = seeded_hasher_builder.hash_one("Goodbye, World");
        assert_ne!(h2, h3);
        println!("h1: {h1}, h2: {h2}, h3: {h3}");
    }

    #[test]
    fn hashers_with_different_seeds() {
        let seeded_hasher_builder1 = SeededHasherBuilder::with_seed(16);
        let h1 = seeded_hasher_builder1.hash_one("Hello, World");

        let seeded_hasher_builder2 = SeededHasherBuilder::with_seed(0xbeef);
        let h2 = seeded_hasher_builder2.hash_one("Hello, World");
        assert_ne!(h1, h2, "hashed value should be different")
    }

    #[test]
    fn hashers_with_same_seed() {
        let seeded_hasher_builder1 = SeededHasherBuilder::with_seed(0xbeef);
        let h1 = seeded_hasher_builder1.hash_one("Hello, World");

        let seeded_hasher_builder2 = SeededHasherBuilder::with_seed(0xbeef);
        let h2 = seeded_hasher_builder2.hash_one("Hello, World");
        assert_eq!(h1, h2, "hashed value should be same")
    }
}
