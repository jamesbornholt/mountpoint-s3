use std::ffi::OsStr;
use std::time::Duration;

use async_trait::async_trait;
use fuser::FileType;
use time::OffsetDateTime;

pub use crate::namespace::error::InodeError;
pub use crate::namespace::expiry::Expiry;

pub mod bucket;
mod cache;
mod error;
mod expiry;

pub type InodeNo = u64;

pub const ROOT_INODE: InodeNo = 1;

/// An inode stores metadata about a file or directory in the file system. Different namespace
/// implementations will have different inode implementations, but they all implement this Inode
/// trait that exposes the basic metadata of the inode to the rest of the file system.
///
/// Inodes need to be cheaply clonable by reference (i.e. should likely be an `Arc<Inner>`).
pub trait Inode: Clone + Send + Sync {
    /// Inode number for this inode
    fn ino(&self) -> InodeNo;

    /// Name of this inode
    fn name(&self) -> &str;

    /// Inode number of the parent of this inode. If this is the root inode, then self.parent() == self.ino() == FUSE_ROOT_INODE
    fn parent(&self) -> InodeNo;

    /// Kind of this inode
    fn kind(&self) -> InodeKind;

    /// S3 key this object corresponds to
    ///
    /// TODO: this doesn't make sense for all namespace implementations or all inode kinds
    fn full_key(&self) -> &str;

    /// Is this inode remote or local?
    ///
    /// TODO: this doesn't make sense for all namespace implementations
    fn is_remote(&self) -> bool;

    /// Error description
    fn description(&self) -> String {
        format!("{} (full key {:?})", self.ino(), self.full_key())
    }
}

/// Inodes are either files or directories. Mountpoint does not support other kinds (symlinks etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    File,
    Directory,
}

impl InodeKind {
    fn as_str(&self) -> &'static str {
        match self {
            InodeKind::File => "file",
            InodeKind::Directory => "directory",
        }
    }
}

impl From<InodeKind> for FileType {
    fn from(kind: InodeKind) -> Self {
        match kind {
            InodeKind::File => FileType::RegularFile,
            InodeKind::Directory => FileType::Directory,
        }
    }
}

/// The stat metadata for an inode.
#[derive(Debug, Clone)]
pub struct InodeStat {
    /// Time this stat becomes invalid and needs to be refreshed
    pub expiry: Expiry,

    /// Size in bytes
    pub size: usize,

    /// Time of last file content modification
    pub mtime: OffsetDateTime,
    /// Time of last file metadata (or content) change
    pub ctime: OffsetDateTime,
    /// Time of last access
    pub atime: OffsetDateTime,
    /// Etag for the file (object)
    pub etag: Option<String>,
    /// Inodes corresponding to S3 objects with GLACIER or DEEP_ARCHIVE storage classes
    /// are only readable after restoration. For objects with other storage classes
    /// this field should be always `true`.
    pub is_readable: bool,
}

impl InodeStat {
    pub fn is_valid(&self) -> bool {
        !self.expiry.is_expired()
    }

    pub fn set_validity(&mut self, validity: Duration) {
        self.expiry = Expiry::from_now(validity);
    }
}

/// The result of a lookup operation on a namespace
#[derive(Debug, Clone)]
pub struct LookedUp<I: Inode> {
    pub inode: I,
    pub stat: InodeStat,
}

impl<I: Inode> LookedUp<I> {
    pub fn validity(&self) -> Duration {
        self.stat.expiry.remaining_ttl()
    }
}

/// A handle for a file open for reading
#[async_trait]
pub trait ReadHandle: Send {
    fn finish(self) -> Result<(), InodeError>;
}

/// A handle for a file open for writing
#[async_trait]
pub trait WriteHandle: Send {
    fn inc_file_size(&self, len: usize);
    fn finish(self) -> Result<(), InodeError>;
}

/// A handle for a readdir stream
#[async_trait]
pub trait ReaddirHandle<I: Inode>: Send {
    async fn next(&self) -> Result<Option<LookedUp<I>>, InodeError>;

    fn readd(&self, entry: LookedUp<I>);

    fn remember(&self, entry: &LookedUp<I>);

    fn parent(&self) -> InodeNo;
}

/// A namespace manages the mapping between a file system tree and its inodes. The file system can
/// query the namespace through `lookup` and `readdir` to discover which inode corresponds to a
/// path. Given an inode, the file system can modify the structure of the namespace with `create`,
/// `unlink`, and `rmdir`; and operate on individual files through `getattr`, `setattr`, `read`, and
/// `write`.
///
/// Namespaces are responsible for maintaining a "lookup count" for each inode, which is incremented
/// every time the inode is returned from `lookup` or when the file system asks to "remember" an
/// inode returned by `readdir`. Whenever the lookup count is non-zero, the file system can call
/// methods that take an `InodeNo` as input without first re-validating that the target file path
/// still corresponds to that inode. The file system calls `forget` to decrement the lookup count
/// when it no longer wants to be able to reference an inode in this way.
#[async_trait]
pub trait Namespace {
    type Inode: Inode;
    type WriteHandle: WriteHandle;
    type ReadHandle: ReadHandle;
    type ReaddirHandle: ReaddirHandle<Self::Inode>;

    async fn lookup(&self, parent_ino: InodeNo, name: &OsStr) -> Result<LookedUp<Self::Inode>, InodeError>;

    async fn getattr(&self, ino: InodeNo, force_revalidate: bool) -> Result<LookedUp<Self::Inode>, InodeError>;

    async fn setattr(
        &self,
        ino: InodeNo,
        atime: Option<OffsetDateTime>,
        mtime: Option<OffsetDateTime>,
    ) -> Result<LookedUp<Self::Inode>, InodeError>;

    async fn create(
        &self,
        dir_ino: InodeNo,
        name: &OsStr,
        kind: InodeKind,
    ) -> Result<LookedUp<Self::Inode>, InodeError>;

    async fn unlink(&self, parent_ino: InodeNo, name: &OsStr) -> Result<(), InodeError>;

    async fn rmdir(&self, parent_ino: InodeNo, name: &OsStr) -> Result<(), InodeError>;

    async fn read(&self, ino: InodeNo) -> Result<Self::ReadHandle, InodeError>;

    async fn write(
        &self,
        ino: InodeNo,
        allow_overwrite: bool,
        is_truncate: bool,
    ) -> Result<Self::WriteHandle, InodeError>;

    async fn readdir(&self, dir_ino: InodeNo, page_size: usize) -> Result<Self::ReaddirHandle, InodeError>;

    async fn forget(&self, ino: InodeNo, n: u64) -> Result<(), InodeError>;
}
