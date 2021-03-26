use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};

use bioyino_metric::{name::MetricName, Metric};

use crate::errors::GeneralError;
use crate::Float;

const SHARDS: usize = 64;

pub type RotatedCacheShard = HashMap<MetricName, Mutex<Metric<Float>>>;
pub type RotatedCache = Vec<RotatedCacheShard>;

#[derive(Clone)]
pub struct SharedCache {
    shards: Arc<[RwLock<HashMap<MetricName, Mutex<Metric<Float>>>>; SHARDS]>,
}

impl SharedCache {
    pub fn new() -> Self {
        Self {
            shards: Arc::new(array_init::array_init(|_| RwLock::new(HashMap::new()))),
        }
    }

    pub fn accumulate(&self, name: MetricName, new: Metric<Float>) -> Result<(), GeneralError> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.name_with_tags());
        let index = hasher.finish() as usize % SHARDS;
        let read = self.shards[index].read().unwrap();
        match read.get(&name) {
            Some(metric) => {
                let mut metric = metric.lock().unwrap();
                metric.accumulate(new)?;
            }
            None => {
                drop(read);
                let mut write = self.shards[index].write().unwrap();
                write.insert(name, Mutex::new(new));
            }
        }
        Ok(())
    }

    pub fn rotate(&self, collect: bool) -> RotatedCache {
        // avoid allocation if it is not required (Vec::new does not allocate)
        let mut rotated = if collect { Vec::with_capacity(SHARDS) } else { Vec::new() };
        for i in 0..SHARDS {
            let mut write = self.shards[i].write().unwrap();
            let mut m = HashMap::with_capacity(write.len() / 2);
            std::mem::swap(write.deref_mut(), &mut m);
            if collect {
                rotated.push(m);
            }
        }
        rotated
    }

    #[cfg(test)]
    pub fn get(&self, name: &MetricName) -> Option<Metric<Float>> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.name_with_tags());
        let index = hasher.finish() as usize % SHARDS;
        let read = self.shards[index].read().unwrap();
        read.get(&name).map(|m| m.lock().unwrap().clone())
    }
}
