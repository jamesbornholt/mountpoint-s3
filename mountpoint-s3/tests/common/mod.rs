//! Functions and types shared across integration test modules.
//! Allow for unused items since this is included independently in each module.
#![allow(dead_code)]

#[cfg(feature = "fuse_tests")]
pub mod fuse;

#[cfg(feature = "s3_tests")]
pub mod s3;

use fuser::{FileAttr, FileType};
use futures::executor::ThreadPool;
use mountpoint_s3::fs::{DirectoryEntry, DirectoryReplier};
use mountpoint_s3::namespace::bucket::{Superblock, SuperblockConfig};
use mountpoint_s3::namespace::Inode;
use mountpoint_s3::prefetch::{default_prefetch, DefaultPrefetcher};
use mountpoint_s3::prefix::Prefix;
use mountpoint_s3::{S3Filesystem, S3FilesystemConfig};
use mountpoint_s3_client::mock_client::{MockClient, MockClientConfig};
use mountpoint_s3_client::ObjectClient;
use mountpoint_s3_crt::common::rust_log_adapter::RustLogAdapter;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

pub type TestS3Filesystem<Client> = S3Filesystem<Superblock<Client>, Client, DefaultPrefetcher<ThreadPool>>;

pub fn make_test_filesystem(
    bucket: &str,
    prefix: &Prefix,
    config: S3FilesystemConfig,
) -> (Arc<MockClient>, TestS3Filesystem<Arc<MockClient>>) {
    let client_config = MockClientConfig {
        bucket: bucket.to_string(),
        part_size: 1024 * 1024,
        ..Default::default()
    };

    let client = Arc::new(MockClient::new(client_config));
    let fs = make_test_filesystem_with_client(client.clone(), bucket, prefix, config);
    (client, fs)
}

pub fn make_test_filesystem_with_client<Client>(
    client: Client,
    bucket: &str,
    prefix: &Prefix,
    config: S3FilesystemConfig,
) -> TestS3Filesystem<Client>
where
    Client: ObjectClient + Send + Sync + Clone + 'static,
{
    let runtime = ThreadPool::builder().pool_size(1).create().unwrap();
    let prefetcher = default_prefetch(runtime, Default::default());
    let superblock_config = SuperblockConfig {
        cache_config: config.cache_config.clone(),
        s3_personality: config.s3_personality,
    };
    let ns = Superblock::new(bucket, prefix, client.clone(), superblock_config);
    S3Filesystem::new(ns, client, prefetcher, bucket, prefix, config)
}

#[track_caller]
pub fn assert_attr(attr: FileAttr, ftype: FileType, size: u64, uid: u32, gid: u32, perm: u16) {
    assert_eq!(attr.kind, ftype);
    assert_eq!(attr.size, size);
    assert_eq!(attr.uid, uid);
    assert_eq!(attr.gid, gid);
    assert_eq!(attr.perm, perm);
}

#[derive(Debug)]
pub struct DirectoryReply<I: Inode> {
    readdir_limit: usize,
    pub entries: VecDeque<DirectoryEntry<I>>,
}

impl<I: Inode> DirectoryReplier<I> for &mut DirectoryReply<I> {
    fn add(&mut self, entry: DirectoryEntry<I>) -> bool {
        if self.readdir_limit > 0 && !self.entries.is_empty() && self.entries.len() % self.readdir_limit == 0 {
            true
        } else {
            self.entries.push_back(entry);
            false
        }
    }
}

impl<I: Inode> DirectoryReply<I> {
    pub fn new(max_entries: usize) -> Self {
        Self {
            readdir_limit: max_entries,
            entries: VecDeque::new(),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl<I: Inode> Default for DirectoryReply<I> {
    fn default() -> Self {
        Self::new(0)
    }
}

pub fn tokio_block_on<F: Future>(future: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(future)
}

/// Enable tracing and CRT logging when running unit tests.
#[ctor::ctor]
fn init_tracing_subscriber() {
    let _ = RustLogAdapter::try_init();
    let _ = tracing_subscriber::fmt::try_init();
}
