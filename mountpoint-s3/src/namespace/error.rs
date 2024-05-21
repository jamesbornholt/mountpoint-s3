use std::ffi::OsString;

use thiserror::Error;
use tracing::Level;

use crate::fs::{self, ToErrno};
use crate::namespace::InodeNo;

#[derive(Debug, Error)]
pub enum InodeError {
    #[error("error from ObjectClient")]
    ClientError(#[source] anyhow::Error),
    #[error("file {0:?} does not exist in parent inode {1}")]
    FileDoesNotExist(String, InodeErrorInfo),
    #[error("inode {0} does not exist")]
    InodeDoesNotExist(InodeNo),
    #[error("invalid file name {0:?}")]
    InvalidFileName(OsString),
    #[error("inode {0} is not a directory")]
    NotADirectory(InodeErrorInfo),
    #[error("inode {0} is a directory")]
    IsDirectory(InodeErrorInfo),
    #[error("file already exists at inode {0}")]
    FileAlreadyExists(InodeErrorInfo),
    #[error("inode {0} is not writable")]
    InodeNotWritable(InodeErrorInfo),
    #[error("Invalid state of inode {0} to be written. Aborting the write.")]
    InodeInvalidWriteStatus(InodeErrorInfo),
    #[error("inode {0} is already being written")]
    InodeAlreadyWriting(InodeErrorInfo),
    #[error("inode {0} is not readable while being written")]
    InodeNotReadableWhileWriting(InodeErrorInfo),
    #[error("inode {0} is not writable while being read")]
    InodeNotWritableWhileReading(InodeErrorInfo),
    #[error("remote directory cannot be removed at inode {0}")]
    CannotRemoveRemoteDirectory(InodeErrorInfo),
    #[error("non-empty directory cannot be removed at inode {0}")]
    DirectoryNotEmpty(InodeErrorInfo),
    #[error("inode {0} cannot be unlinked while being written")]
    UnlinkNotPermittedWhileWriting(InodeErrorInfo),
    #[error("corrupted metadata for inode {0}")]
    CorruptedMetadata(InodeErrorInfo),
    #[error("inode {0} is a remote inode and its attributes cannot be modified")]
    SetAttrNotPermittedOnRemoteInode(InodeErrorInfo),
    #[error("inode {old_inode} for remote key {remote_key:?} is stale, replaced by inode {new_inode}")]
    StaleInode {
        remote_key: String,
        old_inode: InodeErrorInfo,
        new_inode: InodeErrorInfo,
    },
}

pub type InodeErrorInfo = String;

// #[derive(Debug)]
// pub struct InodeErrorInfo {
//     ino: InodeNo,
//     description: String,
// }

// impl Display for InodeErrorInfo {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         write!(f, "{} ({})", self.ino, self.description)
//     }
// }

impl From<InodeError> for fs::Error {
    fn from(err: InodeError) -> Self {
        let errno = err.to_errno();
        fs::Error {
            errno,
            message: String::from("inode error"),
            source: Some(anyhow::anyhow!(err)),
            // We are having WARN as the default level of logging for fuse errors
            level: Level::WARN,
        }
    }
}

impl ToErrno for InodeError {
    fn to_errno(&self) -> libc::c_int {
        match self {
            InodeError::ClientError(_) => libc::EIO,
            InodeError::FileDoesNotExist(_, _) => libc::ENOENT,
            InodeError::InodeDoesNotExist(_) => libc::ENOENT,
            InodeError::InvalidFileName(_) => libc::EINVAL,
            InodeError::NotADirectory(_) => libc::ENOTDIR,
            InodeError::IsDirectory(_) => libc::EISDIR,
            InodeError::FileAlreadyExists(_) => libc::EEXIST,
            // Not obvious what InodeNotWritable, InodeAlreadyWriting, InodeNotReadableWhileWriting should be.
            // EINVAL or EROFS would also be reasonable -- but we'll treat them like sealed files.
            InodeError::InodeNotWritable(_) => libc::EPERM,
            InodeError::InodeInvalidWriteStatus(_) => libc::EPERM,
            InodeError::InodeAlreadyWriting(_) => libc::EPERM,
            InodeError::InodeNotReadableWhileWriting(_) => libc::EPERM,
            InodeError::InodeNotWritableWhileReading(_) => libc::EPERM,
            InodeError::CannotRemoveRemoteDirectory(_) => libc::EPERM,
            InodeError::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            InodeError::UnlinkNotPermittedWhileWriting(_) => libc::EPERM,
            InodeError::CorruptedMetadata(_) => libc::EIO,
            InodeError::SetAttrNotPermittedOnRemoteInode(_) => libc::EPERM,
            InodeError::StaleInode { .. } => libc::ESTALE,
        }
    }
}
