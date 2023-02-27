use std::ffi::OsStr;
use std::time::Duration;
use tracing::instrument;

use crate::fs::{DirectoryReplier, Inode, ReadReplier, S3Filesystem, S3FilesystemConfig};
use crate::future::Spawn;
use crate::sync::Arc;
use fuser::{FileAttr, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use s3_client::ObjectClient;

/// This is just a thin wrapper around [S3Filesystem] that implements the actual `fuser` protocol,
/// so that we can test our actual filesystem implementation without having actual FUSE in the loop.
pub struct S3FuseFilesystem<Client: ObjectClient, Runtime> {
    fs: Arc<S3Filesystem<Client, Runtime>>,
    runtime: Runtime,
}

impl<Client, Runtime> S3FuseFilesystem<Client, Runtime>
where
    Client: ObjectClient + Send + Sync + 'static,
    Runtime: Spawn + Send + Sync + Clone + 'static,
{
    pub fn new(client: Client, runtime: Runtime, bucket: &str, prefix: &str, config: S3FilesystemConfig) -> Self {
        let fs = Arc::new(S3Filesystem::new(client, runtime.clone(), bucket, prefix, config));

        Self { fs, runtime }
    }
}

impl<Client, Runtime> Filesystem for S3FuseFilesystem<Client, Runtime>
where
    Client: ObjectClient + Send + Sync + 'static,
    Runtime: Spawn + Send + Sync + 'static,
{
    #[instrument(level = "debug", skip_all)]
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        let fs = self.fs.clone();
        self.runtime.block_on(fs.init(config))
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=parent, name=?name))]
    fn lookup(&mut self, _req: &Request<'_>, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        let fs = self.fs.clone();
        let name = name.to_owned();
        self.runtime.spawn(async move {
            match fs.lookup(parent, &name).await {
                Ok(entry) => reply.entry(&entry.ttl, &entry.attr, entry.generation),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=ino))]
    fn getattr(&mut self, _req: &Request<'_>, ino: Inode, reply: ReplyAttr) {
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.getattr(ino).await {
                Ok(attr) => reply.attr(&attr.ttl, &attr.attr),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=ino))]
    fn open(&mut self, _req: &Request<'_>, ino: Inode, flags: i32, reply: ReplyOpen) {
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.open(ino, flags).await {
                Ok(opened) => reply.opened(opened.fh, opened.flags),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=ino, fh=fh, offset=offset, size=size))]
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: Inode,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock: Option<u64>,
        reply: ReplyData,
    ) {
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            let mut bytes_sent = 0;

            struct Replied(());

            struct ReplyRead<'a> {
                inner: fuser::ReplyData,
                bytes_sent: &'a mut usize,
            }

            impl ReadReplier for ReplyRead<'_> {
                type Replied = Replied;

                fn data(self, data: &[u8]) -> Replied {
                    self.inner.data(data);
                    *self.bytes_sent = data.len();
                    Replied(())
                }

                fn error(self, error: libc::c_int) -> Replied {
                    self.inner.error(error);
                    Replied(())
                }
            }

            let replier = ReplyRead {
                inner: reply,
                bytes_sent: &mut bytes_sent,
            };
            fs.read(ino, fh, offset, size, flags, lock, replier).await;
            // return value of read is proof a reply was sent

            metrics::counter!("fuse.bytes_read", bytes_sent as u64);
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=parent))]
    fn opendir(&mut self, _req: &Request<'_>, parent: Inode, flags: i32, reply: ReplyOpen) {
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.opendir(parent, flags).await {
                Ok(opened) => reply.opened(opened.fh, opened.flags),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=parent, fh=fh, offset=offset))]
    fn readdir(&mut self, _req: &Request<'_>, parent: Inode, fh: u64, offset: i64, mut reply: fuser::ReplyDirectory) {
        struct ReplyDirectory<'a> {
            inner: &'a mut fuser::ReplyDirectory,
        }

        impl<'a> DirectoryReplier for ReplyDirectory<'a> {
            fn add<T: AsRef<OsStr>>(
                &mut self,
                ino: u64,
                offset: i64,
                name: T,
                attr: FileAttr,
                _generation: u64,
                _ttl: Duration,
            ) -> bool {
                self.inner.add(ino, offset, attr.kind, name)
            }
        }

        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            let replier = ReplyDirectory { inner: &mut reply };

            match fs.readdir(parent, fh, offset, replier).await {
                Ok(_) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=parent, fh=fh, offset=offset))]
    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        struct ReplyDirectoryPlus<'a> {
            inner: &'a mut fuser::ReplyDirectoryPlus,
        }

        impl<'a> DirectoryReplier for ReplyDirectoryPlus<'a> {
            fn add<T: AsRef<OsStr>>(
                &mut self,
                ino: u64,
                offset: i64,
                name: T,
                attr: FileAttr,
                generation: u64,
                ttl: Duration,
            ) -> bool {
                self.inner.add(ino, offset, name, &ttl, &attr, generation)
            }
        }

        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            let replier = ReplyDirectoryPlus { inner: &mut reply };

            match fs.readdir(parent, fh, offset, replier).await {
                Ok(_) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=ino, fh=fh))]
    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.release(ino, fh, flags, lock_owner, flush).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }
}
