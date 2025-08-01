use indicatif::{ProgressBar, ProgressStyle};

pub struct ProgressReporter {
    bar: Option<ProgressBar>,
}

impl ProgressReporter {
    pub fn new(total: Option<u64>) -> Self {
        let bar = total.map(|total| {
            let bar = ProgressBar::new(total);
            bar.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
                )
                .unwrap()
                .progress_chars("##-"),
            );
            bar
        });

        Self { bar }
    }

    pub fn set_message(&self, msg: &str) {
        if let Some(bar) = &self.bar {
            bar.set_message(msg.to_string());
        }
    }

    pub fn inc(&self, delta: u64) {
        if let Some(bar) = &self.bar {
            bar.inc(delta);
        }
    }

    pub fn finish(&self) {
        if let Some(bar) = &self.bar {
            bar.finish();
        }
    }
}
