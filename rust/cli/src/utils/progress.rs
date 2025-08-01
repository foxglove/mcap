use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct ProgressTracker {
    bar: ProgressBar,
}

impl ProgressTracker {
    pub fn new(total: u64, message: &str) -> Self {
        let bar = ProgressBar::new(total);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        bar.set_message(message.to_string());
        bar.enable_steady_tick(Duration::from_millis(120));

        Self { bar }
    }

    pub fn new_spinner(message: &str) -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        bar.set_message(message.to_string());
        bar.enable_steady_tick(Duration::from_millis(120));

        Self { bar }
    }

    pub fn set_position(&self, pos: u64) {
        self.bar.set_position(pos);
    }

    pub fn inc(&self, delta: u64) {
        self.bar.inc(delta);
    }

    pub fn set_message(&self, message: &str) {
        self.bar.set_message(message.to_string());
    }

    pub fn finish_with_message(&self, message: &str) {
        self.bar.finish_with_message(message.to_string());
    }

    pub fn finish(&self) {
        self.bar.finish();
    }
}

impl Drop for ProgressTracker {
    fn drop(&mut self) {
        self.bar.finish_and_clear();
    }
}
