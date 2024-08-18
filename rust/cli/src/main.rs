use std::process::ExitCode;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

mod cli;
mod error;
mod filter;
mod info;
mod mcap;
mod reader;
mod utils;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> ExitCode {
    #[cfg(feature = "timings")]
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("failed to install otel");

    #[cfg(feature = "timings")]
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(
            opentelemetry::trace::TracerProvider::tracer(&provider, "mcap"),
        ))
        .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::from_default_env()))
        .init();

    #[cfg(not(feature = "timings"))]
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::from_default_env()))
        .init();

    let code = if let Err(e) = cli::run().await {
        eprintln!("{e}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    };

    #[cfg(feature = "timings")]
    provider.force_flush();

    code
}
