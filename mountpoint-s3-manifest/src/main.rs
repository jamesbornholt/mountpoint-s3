use std::sync::Arc;

use mountpoint_s3::cli::CliArgs;
use mountpoint_s3::s3::S3Personality;
use mountpoint_s3_manifest::namespace::ManifestNamespace;

/// currently the manifest is hardcoded, and you'll need to specify `giab` as the bucket name to the
/// CLI.
fn main() -> anyhow::Result<()> {
    mountpoint_s3::cli::main(mountpoint_s3::cli::create_s3_client, create_manifest_namespace)
}

pub fn create_manifest_namespace<Client>(
    args: &CliArgs,
    client: Client,
    s3_personality: S3Personality,
) -> anyhow::Result<ManifestNamespace> {
    let keys = vec![
        "s3://giab/README.ftp_structure",
        "s3://giab/README.s3_structure",
        "s3://giab/README_Aspera_download_from_ftp.txt",
        "s3://giab/README_giab_URL_replacement2019.txt",
    ];
    Ok(ManifestNamespace::new(keys.into_iter().map(|k| k.to_string())))
}