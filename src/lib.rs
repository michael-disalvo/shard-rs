use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::Mutex;

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

// TODO: I think the premise is that we can recreate most HashMap methods, but anything that gets a
// reference returned will have to get a LockedRef

impl<K, V, S> Shard<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn internal_shard<Q>(&self, k: &Q) -> &Mutex<HashMap<K, V, S>>
    where
        Q: Hash + ?Sized,
        K: Borrow<Q>,
    {
        let hash = make_hash::<Q, S>(&self.hash_builder, k);
        let index = hash as usize % self.maps.len();
        &self.maps[index]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rand::RngExt;
    use rand::distr::Alphanumeric;

    fn generate_random_string(length: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    #[test]
    fn test_random_strings_get() {
        let shard: Shard<String, String> = Shard::new(3);
        let random_strings: Vec<_> = (0..1000)
            .map(|_| (generate_random_string(50), generate_random_string(30)))
            .collect();
        for (k, v) in &random_strings {
            shard
                .internal_shard(k)
                .lock()
                .unwrap()
                .insert(k.clone(), v.clone());
        }

        for (k, v) in &random_strings {
            assert_eq!(shard.internal_shard(k).lock().unwrap().get(k).unwrap(), v);
        }
    }

    #[test]
    fn basic_usage() {
        let shard: Shard<String, String> = Shard::new(3);

        shard
            .internal_shard("key")
            .lock()
            .unwrap()
            .insert("key".to_string(), "val".to_string());

        assert_eq!(
            shard
                .internal_shard("key")
                .lock()
                .unwrap()
                .get("key")
                .unwrap(),
            "val"
        );
    }
}
