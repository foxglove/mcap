//! Bridges the AWS SDK default credential chain into `object_store`.
//!
//! `object_store` resolves AWS credentials from environment variables and
//! instance/container metadata only; it never reads `~/.aws/credentials` or
//! `~/.aws/config`, so profiles, SSO sessions, and `credential_process` are
//! invisible to it. This module plugs `aws-config`'s default chain — the same
//! resolution the AWS CLI uses, with the same precedence — into an
//! `object_store` `CredentialProvider` for `s3://` inputs.

use std::sync::Arc;

use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::imds::region::ImdsRegionProvider;
use aws_config::meta::region::ProvideRegion as _;
use aws_config::profile::ProfileFileRegionProvider;
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::{ProvideCredentials as _, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential};
use object_store::path::Path as ObjectStorePath;
use object_store::{CredentialProvider, ObjectStore, ObjectStoreScheme};
use url::Url;

/// Build an S3 store for `url`, resolving credentials through the AWS SDK
/// default chain: env vars, `~/.aws/credentials` and `~/.aws/config`
/// (honoring `AWS_PROFILE`), SSO, `credential_process`, web identity,
/// ECS/EKS, and IMDS. `options` (the `AWS_*` env vars) still configure the
/// builder; a region there overrides the SDK chain's region lookup.
pub fn build_s3_store(
    runtime: &tokio::runtime::Runtime,
    url: &Url,
    options: Vec<(String, String)>,
) -> object_store::Result<(Box<dyn ObjectStore>, ObjectStorePath)> {
    let (_, object_path) = ObjectStoreScheme::parse(url)?;
    let mut builder = AmazonS3Builder::new().with_url(url.to_string());
    // The region may also live in ~/.aws/config; ask the SDK chain unless an
    // env var provides it below (builder setters last-write-win).
    if !options_have_region(&options) {
        if let Some(region) = runtime.block_on(default_chain_region()) {
            builder = builder.with_region(region);
        }
    }
    for (key, value) in options {
        if let Ok(key) = key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            builder = builder.with_config(key, value);
        }
    }
    builder = builder.with_credentials(Arc::new(SdkChainCredentialProvider::default_chain()));
    Ok((Box::new(builder.build()?), object_path))
}

fn options_have_region(options: &[(String, String)]) -> bool {
    options.iter().any(|(key, _)| {
        matches!(
            key.to_ascii_lowercase().parse(),
            Ok(AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion)
        )
    })
}

// An env-var region reaches the builder through `options`, so this lookup
// covers the remaining sources the aws CLI consults: ~/.aws/config, then EC2
// instance metadata. The IMDS probe runs only when nothing else set a region
// and honors AWS_EC2_METADATA_DISABLED; a single attempt keeps the off-EC2
// worst case to about a second (its connect timeout), while on EC2 the first
// attempt answers in milliseconds.
async fn default_chain_region() -> Option<String> {
    let profile = ProfileFileRegionProvider::builder().build();
    let config = sdk_provider_config();
    let imds_client = aws_config::imds::Client::builder()
        .configure(&config)
        .max_attempts(1)
        .build();
    let imds = ImdsRegionProvider::builder()
        .configure(&config)
        .imds_client(imds_client)
        .build();
    region_from_providers(&profile, &imds).await
}

async fn region_from_providers(
    profile: &ProfileFileRegionProvider,
    imds: &ImdsRegionProvider,
) -> Option<String> {
    if let Some(region) = profile.region().await {
        return Some(region.to_string());
    }
    imds.region().await.map(|region| region.to_string())
}

// The SDK needs an HTTP client for its network-backed providers (SSO, STS,
// IMDS). Supply one on the `ring` crypto backend: the default `aws-lc`
// backend compiles a C library via cmake, which the release cross-builds
// don't have, and `object_store`'s reqwest already uses `ring` anyway.
// Built once and cloned (the clone shares the underlying client), so TLS
// roots load a single time across the region lookup and credentials chain.
fn sdk_provider_config() -> ProviderConfig {
    static CONFIG: std::sync::OnceLock<ProviderConfig> = std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            ProviderConfig::default().with_http_client(
                aws_smithy_http_client::Builder::new()
                    .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                        aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
                    ))
                    .build_https(),
            )
        })
        .clone()
}

/// `object_store` credential provider backed by an AWS SDK provider —
/// the default chain in production. The chain caches credentials and
/// refreshes expiring ones internally, so long downloads keep working
/// across temporary-credential rotations.
pub struct SdkChainCredentialProvider {
    provider: tokio::sync::OnceCell<SharedCredentialsProvider>,
    // Tests inject a provider here; None means build the default chain on
    // first use (construction is async, so it can't happen in `new`).
    preset: Option<SharedCredentialsProvider>,
}

impl SdkChainCredentialProvider {
    pub fn default_chain() -> Self {
        Self {
            provider: tokio::sync::OnceCell::new(),
            preset: None,
        }
    }

    #[cfg(test)]
    fn with_provider(provider: SharedCredentialsProvider) -> Self {
        Self {
            provider: tokio::sync::OnceCell::new(),
            preset: Some(provider),
        }
    }

    async fn provider(&self) -> &SharedCredentialsProvider {
        self.provider
            .get_or_init(|| async {
                if let Some(preset) = &self.preset {
                    return preset.clone();
                }
                SharedCredentialsProvider::new(
                    DefaultCredentialsChain::builder()
                        .configure(sdk_provider_config())
                        .build()
                        .await,
                )
            })
            .await
    }
}

impl std::fmt::Debug for SdkChainCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SdkChainCredentialProvider")
    }
}

#[async_trait]
impl CredentialProvider for SdkChainCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let credentials = self
            .provider()
            .await
            .provide_credentials()
            .await
            .map_err(|source| object_store::Error::Generic {
                store: "S3",
                source: format!(
                    "no AWS credentials found (checked env vars, ~/.aws profiles, SSO, \
                     and instance metadata): {source}"
                )
                .into(),
            })?;
        Ok(Arc::new(to_aws_credential(&credentials)))
    }
}

fn to_aws_credential(credentials: &aws_credential_types::Credentials) -> AwsCredential {
    AwsCredential {
        key_id: credentials.access_key_id().to_string(),
        secret_key: credentials.secret_access_key().to_string(),
        token: credentials.session_token().map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::profile::ProfileFileCredentialsProvider;
    use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
    use object_store::ObjectStoreExt as _;
    use std::io::Write as _;

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime")
    }

    // Build a provider that reads a real (temp) credentials file through the
    // SDK's profile machinery, isolated from the ambient env and ~/.aws.
    fn file_provider(
        contents: &str,
        profile: &str,
    ) -> (SdkChainCredentialProvider, tempfile::NamedTempFile) {
        let mut file = tempfile::NamedTempFile::new().expect("temp credentials file");
        file.write_all(contents.as_bytes())
            .expect("write credentials");
        let files = EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, file.path())
            .build();
        let sdk_provider = ProfileFileCredentialsProvider::builder()
            .configure(&sdk_provider_config())
            .profile_files(files)
            .profile_name(profile)
            .build();
        (
            SdkChainCredentialProvider::with_provider(SharedCredentialsProvider::new(sdk_provider)),
            file,
        )
    }

    #[test]
    fn maps_sdk_credentials_including_session_token() {
        let credentials = aws_credential_types::Credentials::new(
            "akid",
            "secret",
            Some("token".into()),
            None,
            "test",
        );
        let mapped = to_aws_credential(&credentials);
        assert_eq!(mapped.key_id, "akid");
        assert_eq!(mapped.secret_key, "secret");
        assert_eq!(mapped.token.as_deref(), Some("token"));
    }

    #[test]
    fn provides_credentials_from_file_profile() {
        let (provider, _file) = file_provider(
            "[myprofile]\n\
             aws_access_key_id = AKIAFROMFILE\n\
             aws_secret_access_key = filesecret\n\
             aws_session_token = filetoken\n",
            "myprofile",
        );
        let credential = test_runtime()
            .block_on(provider.get_credential())
            .expect("file profile should provide credentials");
        assert_eq!(credential.key_id, "AKIAFROMFILE");
        assert_eq!(credential.secret_key, "filesecret");
        assert_eq!(credential.token.as_deref(), Some("filetoken"));
    }

    #[test]
    fn missing_credentials_error_is_descriptive() {
        let (provider, _file) = file_provider("", "default");
        let err = test_runtime()
            .block_on(provider.get_credential())
            .expect_err("empty credentials file should yield no credentials");
        assert!(
            err.to_string().contains("no AWS credentials found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn requests_are_signed_with_file_profile_credentials() {
        use std::io::{BufRead as _, BufReader, Write as _};
        use std::net::TcpListener;

        // Minimal S3 endpoint: capture the request head, return one byte.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("addr");
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream);
            let mut head = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("read request line");
                if line == "\r\n" {
                    break;
                }
                head.push_str(&line);
            }
            let mut stream = reader.into_inner();
            stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\n\
                      Content-Range: bytes 0-0/1\r\nConnection: close\r\n\r\nx",
                )
                .expect("write response");
            head
        });

        let (provider, _file) = file_provider(
            "[signing]\n\
             aws_access_key_id = AKIAFROMFILE\n\
             aws_secret_access_key = filesecret\n",
            "signing",
        );
        let store = AmazonS3Builder::new()
            .with_bucket_name("test-bucket")
            .with_region("us-east-2")
            .with_endpoint(format!("http://{addr}"))
            .with_allow_http(true)
            .with_credentials(Arc::new(provider))
            .build()
            .expect("build store");

        let runtime = test_runtime();
        runtime
            .block_on(store.get_range(&ObjectStorePath::from("demo.mcap"), 0..1))
            .expect("ranged get against test server");
        let head = server.join().expect("server thread");
        assert!(
            head.contains("AKIAFROMFILE"),
            "request should be SigV4-signed with the file profile's key id, got:\n{head}"
        );
    }

    // Minimal IMDSv2 endpoint: answers the token PUT, then serves the
    // region path. Returns the URL and a served-request counter.
    fn serve_fake_imds(
        region: &'static str,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::{BufRead as _, BufReader, Write as _};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake imds");
        let addr = listener.local_addr().expect("addr");
        let requests = std::sync::Arc::new(AtomicUsize::new(0));
        let server_requests = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let stream = stream.expect("accept");
                let mut reader = BufReader::new(stream);
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
                    continue;
                }
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                server_requests.fetch_add(1, Ordering::SeqCst);
                // Token responses must echo a TTL header or the client
                // rejects them.
                let (body, extra_headers) = if request_line.starts_with("PUT") {
                    (
                        "fake-imds-token",
                        "x-aws-ec2-metadata-token-ttl-seconds: 21600\r\n",
                    )
                } else {
                    (region, "")
                };
                let mut stream = reader.into_inner();
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write imds response");
            }
        });
        (format!("http://{addr}"), requests)
    }

    fn profile_region_provider(
        config_contents: &str,
    ) -> (ProfileFileRegionProvider, tempfile::NamedTempFile) {
        let mut file = tempfile::NamedTempFile::new().expect("temp config file");
        file.write_all(config_contents.as_bytes())
            .expect("write config");
        let files = EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Config, file.path())
            .build();
        (
            ProfileFileRegionProvider::builder()
                .profile_files(files)
                .build(),
            file,
        )
    }

    fn imds_region_provider(endpoint: &str) -> ImdsRegionProvider {
        let client = aws_config::imds::Client::builder()
            .configure(&sdk_provider_config())
            .endpoint(endpoint)
            .expect("imds endpoint")
            .max_attempts(1)
            .build();
        ImdsRegionProvider::builder()
            .configure(&sdk_provider_config())
            .imds_client(client)
            .build()
    }

    #[test]
    fn region_falls_back_to_imds_when_profile_has_none() {
        let (imds_url, _requests) = serve_fake_imds("us-west-2");
        let (profile, _file) = profile_region_provider("");
        let imds = imds_region_provider(&imds_url);
        let region = test_runtime().block_on(super::region_from_providers(&profile, &imds));
        assert_eq!(region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn profile_region_wins_without_touching_imds() {
        let (imds_url, requests) = serve_fake_imds("us-west-2");
        let (profile, _file) = profile_region_provider("[default]\nregion = eu-west-3\n");
        let imds = imds_region_provider(&imds_url);
        let region = test_runtime().block_on(super::region_from_providers(&profile, &imds));
        assert_eq!(region.as_deref(), Some("eu-west-3"));
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "IMDS should not be consulted when the profile defines a region"
        );
    }

    #[test]
    fn build_s3_store_parses_bucket_and_key() {
        let runtime = test_runtime();
        let url = Url::parse("s3://my-bucket/dir/demo.mcap").expect("url");
        // A region in the options keeps this hermetic: the SDK region chain
        // (which can probe IMDS) is never consulted.
        let options = vec![("AWS_REGION".to_string(), "us-east-2".to_string())];
        let (_, path) = build_s3_store(&runtime, &url, options).expect("build store");
        assert_eq!(path.as_ref(), "dir/demo.mcap");
    }
}
