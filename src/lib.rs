use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

pub use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

pub struct Shard<K, V, S = RandomState> {
    maps: Box<[Mutex<HashMap<K, V, S>>]>,
    hash_builder: S,
}

fn make_hash<Q, S>(hash_builder: &S, val: &Q) -> u64
where
    Q: Hash + ?Sized,
    S: BuildHasher,
{
    hash_builder.hash_one(val)
}

impl<K, V> Shard<K, V> {
    pub fn new(num_shards: usize) -> Self {
        Self {
            maps: (0..num_shards).map(|_| Default::default()).collect(),
            hash_builder: Default::default(),
        }
    }
}

impl<K, V, S> Shard<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the hashmap that may hold they key, does not lock
    pub fn internal_shard<Q>(&self, k: &Q) -> &Mutex<HashMap<K, V, S>>
    where
        Q: Hash + ?Sized,
    {
        let hash = make_hash::<Q, S>(&self.hash_builder, k);
        let index = hash as usize % self.maps.len();
        &self.maps[index]
    }

    pub fn get<Q>(&self, k: &Q) -> Option<MappedMutexGuard<'_, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        MutexGuard::try_map(self.internal_shard(k).lock(), |hm| hm.get_mut(k)).ok()
    }

    pub fn insert(&self, k: K, v: V) -> Option<V> {
        self.internal_shard(&k).lock().insert(k, v)
    }

    pub fn clear(&self) {
        for m in &self.maps {
            m.lock().clear();
        }
    }

    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.internal_shard(k).lock().contains_key(k)
    }

    pub fn get_or_insert_with(&self, k: K, default: impl FnOnce() -> V) -> MappedMutexGuard<'_, V> {
        let locked_shard = self.internal_shard(&k).lock();
        MutexGuard::map(locked_shard, |hm| hm.entry(k).or_insert_with(default))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rand::RngExt;
    use rand::distr::Alphanumeric;
    use std::ops::Deref;

    fn generate_random_string(length: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    #[test]
    fn test_get_or_insert_with() {
        let shard = Shard::new(4);

        assert_eq!(*shard.get_or_insert_with("key1", || "val1").deref(), "val1");
        assert_eq!(*shard.get_or_insert_with("key1", || "val2").deref(), "val1");

        assert_eq!(shard.get("key1").as_deref().copied(), Some("val1"));
    }

    #[test]
    fn test_insert_and_get() {
        let shard = Shard::new(3);

        assert_eq!(None, shard.insert("key1", "val1"));

        assert_eq!(None, shard.insert("key2", "val2"));
        assert_eq!(Some("val2"), shard.insert("key2", "val3"));

        assert_eq!(shard.get("key1").as_deref().copied(), Some("val1"));
        assert_eq!(shard.get("key2").as_deref().copied(), Some("val3"));

        assert_eq!(shard.get("key3").as_deref().copied(), None);
    }

    #[test]
    fn test_get_as_get_mut() {
        let shard = Shard::new(3);

        shard.insert("key1", "val1".to_string());

        if let Some(mut s) = shard.get("key1") {
            s.push_str("_extra");
        }

        assert_eq!(
            shard.get("key1").as_deref(),
            Some("val1_extra".to_string()).as_ref()
        );
    }

    #[test]
    fn test_random_strings_get() {
        let shard: Shard<String, String> = Shard::new(3);
        let random_strings: Vec<_> = (0..1000)
            .map(|_| (generate_random_string(50), generate_random_string(30)))
            .collect();
        for (k, v) in &random_strings {
            shard.internal_shard(k).lock().insert(k.clone(), v.clone());
        }

        for (k, v) in &random_strings {
            assert_eq!(shard.internal_shard(k).lock().get(k).unwrap(), v);
        }
    }

    #[test]
    fn basic_usage() {
        let shard: Shard<String, String> = Shard::new(3);

        shard
            .internal_shard("key")
            .lock()
            .insert("key".to_string(), "val".to_string());

        assert_eq!(
            shard.internal_shard("key").lock().get("key").unwrap(),
            "val"
        );
    }
}
