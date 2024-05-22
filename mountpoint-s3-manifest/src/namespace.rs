use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mountpoint_s3::namespace::{self, Inode as _, *};
use time::OffsetDateTime;

#[derive(Debug)]
pub struct ManifestNamespace {
    inodes: HashMap<InodeNo, Inode>,
}

#[derive(Debug, Clone)]
pub struct Inode {
    inner: Arc<InodeInner>,
}

#[derive(Debug)]
struct InodeInner {
    ino: InodeNo,
    name: String,
    parent: InodeNo,
    kind: InodeKind,
}

#[derive(Debug)]
enum InodeKind {
    File { bucket: String, key: String, size: usize },
    Directory { key: String, children: BTreeMap<String, InodeNo> },
}

impl Inode {
    fn stat(&self) -> InodeStat {
        let (size, etag) = match self.inner.kind {
            InodeKind::File { size, .. } => (size, Some("not real".into())),
            InodeKind::Directory { .. } => (0, None),
        };
        InodeStat {
            expiry: Expiry::from_now(Duration::from_secs(60 * 60 * 24 * 1000)),
            size,
            mtime: OffsetDateTime::UNIX_EPOCH,
            ctime: OffsetDateTime::UNIX_EPOCH,
            atime: OffsetDateTime::UNIX_EPOCH,
            etag,
            is_readable: true,
        }
    }
}

impl ManifestNamespace {
    pub fn new(s3_uris: impl Iterator<Item = String>) -> Self {
        // This implementation is a bit dumb but I'm too lazy to work out how to make the borrow
        // checker happy in a one-pass algorithm

        #[derive(Debug)]
        struct File {
            bucket: String,
            key: String,
        }

        #[derive(Debug)]
        enum TreeNode {
            File(File),
            Directory(BTreeMap<String, TreeNode>),
        }

        // Phase 1: build the tree structure
        let mut tree = BTreeMap::new();
        for mut uri in s3_uris {
            assert!(uri.starts_with("s3://"));
            let uri = uri.split_off("s3://".len());
            let (bucket, path) = uri.split_once('/').expect("must have a bucket");
            let mut components = path.split('/').peekable();
            let mut current = &mut tree;
            while let Some(component) = components.next() {
                if components.peek().is_some() {
                    let new_node = current
                        .entry(component.to_string())
                        .or_insert_with(|| TreeNode::Directory(BTreeMap::new()));
                    let TreeNode::Directory(new_tree) = new_node else {
                        unreachable!("must be a directory");
                    };
                    current = new_tree;
                } else {
                    current.insert(
                        component.to_string(),
                        TreeNode::File(File {
                            bucket: bucket.to_string(),
                            key: path.to_string(),
                        }),
                    );
                }
            }
        }

        fn walk(
            inodes: &mut HashMap<InodeNo, Inode>,
            next_ino: &mut InodeNo,
            node: TreeNode,
            name: &str,
            parent: InodeNo,
        ) -> InodeNo {
            let ino = *next_ino;
            *next_ino += 1;
            let inode_kind = match node {
                TreeNode::Directory(children) => {
                    let children = children
                        .into_iter()
                        .map(|(name, node)| {
                            let ino = walk(inodes, next_ino, node, &name, ino);
                            (name, ino)
                        })
                        .collect::<BTreeMap<_, _>>();
                    InodeKind::Directory { key: name.to_owned(), children }
                }
                TreeNode::File(file) => InodeKind::File {
                    bucket: file.bucket,
                    key: file.key,
                    size: 1024,
                },
            };
            let inode = InodeInner {
                ino,
                name: name.to_string(),
                parent,
                kind: inode_kind,
            };
            let inode = Inode { inner: Arc::new(inode) };
            inodes.insert(ino, inode);
            ino
        }
        let mut inodes = HashMap::new();
        let mut next_ino = ROOT_INODE;
        let root = walk(&mut inodes, &mut next_ino, TreeNode::Directory(tree), "", ROOT_INODE);
        assert_eq!(root, ROOT_INODE);

        Self { inodes }
    }
}

#[derive(Debug)]
pub struct ReadHandle;

impl namespace::ReadHandle for ReadHandle {
    fn finish(self) -> Result<(), InodeError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct WriteHandle;

impl namespace::WriteHandle for WriteHandle {
    fn inc_file_size(&self, _len: usize) {
        unreachable!("doesn't support writes")
    }

    fn finish(self) -> Result<(), InodeError> {
        unreachable!("doesn't support writes")
    }
}

#[derive(Debug)]
pub struct ReaddirHandle(InodeNo, Mutex<Vec<Inode>>);

#[async_trait]
impl namespace::ReaddirHandle<Inode> for ReaddirHandle {
    async fn next(&self) -> Result<Option<LookedUp<Inode>>, InodeError> {
        let Some(next) = self.1.lock().unwrap().pop() else {
            return Ok(None);
        };
        let stat = next.stat();
        Ok(Some(LookedUp { inode: next, stat }))
    }

    fn readd(&self, entry: LookedUp<Inode>) {
        self.1.lock().unwrap().push(entry.inode);
    }

    fn remember(&self, _entry: &LookedUp<Inode>) {
        // no-op
    }

    fn parent(&self) -> InodeNo {
        self.0
    }
}

impl namespace::Inode for Inode {
    fn ino(&self) -> InodeNo {
        self.inner.ino
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn parent(&self) -> InodeNo {
        self.inner.parent
    }

    fn kind(&self) -> namespace::InodeKind {
        match self.inner.kind {
            InodeKind::File { .. } => namespace::InodeKind::File,
            InodeKind::Directory { .. } => namespace::InodeKind::Directory,
        }
    }

    fn full_key(&self) -> &str {
        let InodeKind::File { key, .. } = &self.inner.kind else {
            panic!("can't get full key for a directory");
        };
        key
    }

    fn is_remote(&self) -> bool {
        true
    }
}

#[async_trait]
impl Namespace for ManifestNamespace {
    type Inode = Inode;
    type WriteHandle = WriteHandle;
    type ReadHandle = ReadHandle;
    type ReaddirHandle = ReaddirHandle;

    async fn lookup(&self, parent_ino: InodeNo, name: &OsStr) -> Result<LookedUp<Self::Inode>, InodeError> {
        let name = name
            .to_str()
            .ok_or_else(|| InodeError::InvalidFileName(name.to_owned()))?;

        let parent_inode = self
            .inodes
            .get(&parent_ino)
            .ok_or(InodeError::InodeDoesNotExist(parent_ino))?;
        let InodeKind::Directory { children, .. } = &parent_inode.inner.kind else {
            return Err(InodeError::NotADirectory(parent_inode.description()));
        };
        let ino = children.get(name).ok_or(InodeError::FileDoesNotExist(
            name.to_string(),
            parent_inode.description(),
        ))?;
        let inode = self.inodes.get(ino).ok_or(InodeError::InodeDoesNotExist(*ino))?;
        let stat = inode.stat();
        Ok(LookedUp {
            inode: inode.clone(),
            stat,
        })
    }

    async fn getattr(&self, ino: InodeNo, _force_revalidate: bool) -> Result<LookedUp<Self::Inode>, InodeError> {
        let inode = self.inodes.get(&ino).ok_or(InodeError::InodeDoesNotExist(ino))?;
        let stat = inode.stat();
        Ok(LookedUp {
            inode: inode.clone(),
            stat,
        })
    }

    async fn setattr(
        &self,
        ino: InodeNo,
        _atime: Option<OffsetDateTime>,
        _mtime: Option<OffsetDateTime>,
    ) -> Result<LookedUp<Self::Inode>, InodeError> {
        let inode = self.inodes.get(&ino).ok_or(InodeError::InodeDoesNotExist(ino))?;
        Err(InodeError::InodeNotWritable(inode.description()))
    }

    async fn create(
        &self,
        dir_ino: InodeNo,
        _name: &OsStr,
        _kind: namespace::InodeKind,
    ) -> Result<LookedUp<Self::Inode>, InodeError> {
        let inode = self
            .inodes
            .get(&dir_ino)
            .ok_or(InodeError::InodeDoesNotExist(dir_ino))?;
        Err(InodeError::InodeNotWritable(inode.description()))
    }

    async fn unlink(&self, parent_ino: InodeNo, _name: &OsStr) -> Result<(), InodeError> {
        let inode = self
            .inodes
            .get(&parent_ino)
            .ok_or(InodeError::InodeDoesNotExist(parent_ino))?;
        Err(InodeError::InodeNotWritable(inode.description()))
    }

    async fn rmdir(&self, parent_ino: InodeNo, _name: &OsStr) -> Result<(), InodeError> {
        let inode = self
            .inodes
            .get(&parent_ino)
            .ok_or(InodeError::InodeDoesNotExist(parent_ino))?;
        Err(InodeError::InodeNotWritable(inode.description()))
    }

    async fn read(&self, ino: InodeNo) -> Result<Self::ReadHandle, InodeError> {
        let _inode = self.inodes.get(&ino).ok_or(InodeError::InodeDoesNotExist(ino))?;
        Ok(ReadHandle)
    }

    async fn write(
        &self,
        ino: InodeNo,
        _allow_overwrite: bool,
        _is_truncate: bool,
    ) -> Result<Self::WriteHandle, InodeError> {
        let inode = self.inodes.get(&ino).ok_or(InodeError::InodeDoesNotExist(ino))?;
        Err(InodeError::InodeNotWritable(inode.description()))
    }

    async fn readdir(&self, dir_ino: InodeNo, _page_size: usize) -> Result<Self::ReaddirHandle, InodeError> {
        let dir_inode = self
            .inodes
            .get(&dir_ino)
            .ok_or(InodeError::InodeDoesNotExist(dir_ino))?;
        let InodeKind::Directory { children, .. } = &dir_inode.inner.kind else {
            return Err(InodeError::NotADirectory(dir_inode.description()));
        };
        let mut children = children
            .values()
            .map(|ino| self.inodes.get(ino).cloned().ok_or(InodeError::InodeDoesNotExist(*ino)))
            .collect::<Result<Vec<_>, _>>()?;
        children.reverse();
        Ok(ReaddirHandle(dir_ino, Mutex::new(children)))
    }

    async fn forget(&self, _ino: InodeNo, _n: u64) -> Result<(), InodeError> {
        // no-op
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let uris = vec![
            "s3://bucket/key1",
            "s3://bucket/key2/a",
            "s3://bucket/key2/b",
            "s3://bucket/key3",
            "s3://bucket/key4/a/b",
            "s3://bucket/key4/a/c",
        ];

        let namespace = ManifestNamespace::new(uris.iter().map(|u| u.to_string()));

        println!("{namespace:#?}");
    }
}
