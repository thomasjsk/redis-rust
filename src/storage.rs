use std::collections::HashMap;
use chrono::{Utc};

pub struct StorageRecord {
    pub value: String,
    pub expires_at: i64,
}
impl StorageRecord {
    pub fn new(v: String, exp: i64) -> Self {
        StorageRecord {
            value: v,
            expires_at: exp,
        }
    }
}


pub struct Storage {
    pub _set: HashMap<String, StorageRecord>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            _set: HashMap::new()
        }
    }

    pub fn set(&mut self, kv: (String, String), exp_at: i64) {
        self._set.insert(kv.0.clone(), StorageRecord::new(kv.1.clone(), exp_at));
    }

    pub fn get(&mut self, k: &str) -> Option<&StorageRecord> {
        let record_option = self._set.remove(k);

        if let Some(record) = record_option {
            let expires = record.expires_at;
            let now = Utc::now().timestamp_millis();
            println!("Get...........");
            println!("key: {}; now: {}; expires: {}; diff: {}", k, now, expires, expires - now);
            println!(".................");

            if expires == 0 || (expires > now) {
                println!("not expired yet");
                self._set.insert(k.to_string(), record);
            } else {
                return None;
            }

            return Some(&self._set.get(k).unwrap());
        }

        None
    }
}
