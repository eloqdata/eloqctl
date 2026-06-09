use crate::config::config_base::VersionRow;
use crate::config::deployment::{version_digits, Product};
use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::Duration;

const GITHUB_RELEASES_API: &str =
    "https://api.github.com/repos/eloqdata/eloqkv/releases?per_page=100";
const ELOQKV_REPO: &str = "eloqdata/eloqkv";
const GITHUB_API_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRelease {
    pub tag_name: String,
    pub draft: bool,
    pub prerelease: bool,
    #[serde(default)]
    pub assets: Vec<GitHubAsset>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubAsset {
    pub name: String,
    pub browser_download_url: String,
    pub digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAssetName {
    pub product: String,
    pub version: String,
    pub store: String,
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone)]
pub struct ReleaseAssetMatch {
    pub version: String,
    pub url: String,
    pub digest: Option<String>,
}

pub async fn fetch_eloqkv_releases(client: &reqwest::Client) -> Result<Vec<GitHubRelease>> {
    let response = client
        .get(GITHUB_RELEASES_API)
        .timeout(GITHUB_API_TIMEOUT)
        .header(reqwest::header::USER_AGENT, "eloqctl")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await?;

    if !response.status().is_success() {
        bail!("fetch GitHub releases failed: {}", response.status());
    }

    let releases = response.json::<Vec<GitHubRelease>>().await?;
    Ok(releases
        .into_iter()
        .filter(|release| !release.draft && !release.prerelease)
        .collect())
}

pub fn list_versions_from_releases(
    releases: &[GitHubRelease],
    product: Option<Product>,
    store: Option<&str>,
) -> Vec<VersionRow> {
    let requested_product = product.unwrap_or(Product::EloqKV);
    let requested_product_name = requested_product.name();
    let mut rows = BTreeSet::new();

    for release in releases {
        let release_version = normalize_tag(&release.tag_name);
        if version_digits(&release_version).is_err() {
            continue;
        }

        for asset in &release.assets {
            let Some(parsed) = parse_asset_name(&asset.name) else {
                continue;
            };
            if parsed.product != requested_product_name {
                continue;
            }
            if parsed.version != release_version {
                continue;
            }
            if store.is_some_and(|requested| requested != parsed.store) {
                continue;
            }

            rows.insert((parsed.product, parsed.store, parsed.version));
        }
    }

    rows.into_iter()
        .map(|(product, store, version)| VersionRow {
            product,
            store,
            version,
        })
        .collect()
}

pub fn find_product_asset(
    releases: &[GitHubRelease],
    product: &str,
    version: &str,
    store: &str,
    os: &str,
    arch: &str,
) -> Result<ReleaseAssetMatch> {
    let normalized_version = normalize_tag(version);

    for release in releases {
        if normalize_tag(&release.tag_name) != normalized_version {
            continue;
        }

        for asset in &release.assets {
            let Some(parsed) = parse_asset_name(&asset.name) else {
                continue;
            };
            if parsed.product == product
                && parsed.version == normalized_version
                && parsed.store == store
                && parsed.os == os
                && parsed.arch == arch
            {
                return Ok(ReleaseAssetMatch {
                    version: normalized_version,
                    url: asset.browser_download_url.clone(),
                    digest: asset.digest.clone(),
                });
            }
        }
    }

    Err(anyhow!(
        "no GitHub release asset found for product={}, version={}, store={}, os={}, arch={} in {}",
        product,
        normalized_version,
        store,
        os,
        arch,
        ELOQKV_REPO
    ))
}

pub fn find_eloqkv_asset(
    releases: &[GitHubRelease],
    product: Product,
    version: &str,
    store: &str,
    os: &str,
    arch: &str,
) -> Result<ReleaseAssetMatch> {
    find_product_asset(releases, product.name(), version, store, os, arch)
}

pub fn normalize_tag(tag: &str) -> String {
    tag.trim_start_matches('v').to_string()
}

pub fn parse_asset_name(name: &str) -> Option<ParsedAssetName> {
    let stem = name.strip_suffix(".tar.gz")?;
    let parts = stem.split('-').collect::<Vec<_>>();
    if parts.len() < 5 {
        return None;
    }

    let product_len = match parts.first().copied() {
        Some("eloqkv") => 1,
        Some("log") if parts.get(1).copied() == Some("service") => 2,
        _ => return None,
    };
    let product = parts[..product_len].join("-");
    let version = parts.get(product_len)?.to_string();
    let os = parts.get(parts.len() - 2)?.to_string();
    let arch = parts.last()?.to_string();
    let store = parts[product_len + 1..parts.len() - 2].join("-");
    if store.is_empty() {
        return None;
    }

    Some(ParsedAssetName {
        product,
        version,
        store,
        os,
        arch,
    })
}

#[cfg(test)]
mod tests {
    use super::{list_versions_from_releases, parse_asset_name, GitHubAsset, GitHubRelease};
    use crate::config::deployment::Product;

    #[test]
    fn parse_asset_name_handles_rocks_s3() {
        let asset = parse_asset_name("eloqkv-1.2.2-rocks_s3-ubuntu24-amd64.tar.gz").unwrap();
        assert_eq!(asset.product, "eloqkv");
        assert_eq!(asset.version, "1.2.2");
        assert_eq!(asset.store, "rocks_s3");
        assert_eq!(asset.os, "ubuntu24");
        assert_eq!(asset.arch, "amd64");
    }

    #[test]
    fn parse_asset_name_handles_log_service() {
        let asset = parse_asset_name("log-service-1.2.2-rocksdb-ubuntu24-amd64.tar.gz").unwrap();
        assert_eq!(asset.product, "log-service");
        assert_eq!(asset.store, "rocksdb");
    }

    #[test]
    fn list_versions_aggregates_assets() {
        let releases = vec![GitHubRelease {
            tag_name: "1.2.2".to_string(),
            draft: false,
            prerelease: false,
            assets: vec![
                GitHubAsset {
                    name: "eloqkv-1.2.2-rocksdb-ubuntu24-amd64.tar.gz".to_string(),
                    browser_download_url: "https://example.invalid/rocksdb".to_string(),
                    digest: Some("sha256:abc".to_string()),
                },
                GitHubAsset {
                    name: "eloqkv-1.2.2-rocks_s3-ubuntu24-amd64.tar.gz".to_string(),
                    browser_download_url: "https://example.invalid/rocks_s3".to_string(),
                    digest: Some("sha256:def".to_string()),
                },
            ],
        }];

        let rows = list_versions_from_releases(&releases, Some(Product::EloqKV), None);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.store == "rocksdb"));
        assert!(rows.iter().any(|row| row.store == "rocks_s3"));
    }
}
