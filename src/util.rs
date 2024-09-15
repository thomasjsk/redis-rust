use rand::{thread_rng, Rng};
use rand::distr::Alphanumeric;

pub fn generate_random_string(size: i32) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(usize::try_from(size).unwrap_or_else(|_| 40))
        .map(char::from)
        .collect()
}