use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::io::{Cursor, Read, Write};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use std::vec::IntoIter;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fuse3::path::prelude::*;
use fuse3::{Errno, MountOptions, Result};
use futures_util::stream::{Empty, Iter};
use futures_util::{stream, StreamExt};
use libc::mode_t;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{debug, Level};
use tracing::{error, instrument};
use tracing_subscriber::EnvFilter;

const TTL: Duration = Duration::from_secs(1);
const SEPARATOR: char = '/';

#[derive(Debug)]
enum Entry {
    Dir(Dir),
    File(File),
}

impl Entry {
    fn attr(&self) -> FileAttr {
        match self {
            Entry::Dir(dir) => FileAttr {
                size: 0,
                blocks: 0,
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                kind: FileType::Directory,
                perm: fuse3::perm_from_mode_and_kind(FileType::Directory, dir.mode),
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                crtime: SystemTime::UNIX_EPOCH,
                flags: 0,
            },

            Entry::File(file) => FileAttr {
                size: file.content.len() as _,
                blocks: 0,
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                crtime: SystemTime::UNIX_EPOCH,
                kind: FileType::RegularFile,
                perm: fuse3::perm_from_mode_and_kind(FileType::RegularFile, file.mode),
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            },
        }
    }

    fn set_attr(&mut self, set_attr: SetAttr) -> FileAttr {
        match self {
            Entry::Dir(dir) => {
                if let Some(mode) = set_attr.mode {
                    dir.mode = mode;
                }
            }

            Entry::File(file) => {
                if let Some(size) = set_attr.size {
                    file.content.truncate(size as _);
                }

                if let Some(mode) = set_attr.mode {
                    file.mode = mode;
                }
            }
        }

        self.attr()
    }

    fn is_dir(&self) -> bool {
        matches!(self, Entry::Dir(_))
    }

    fn is_file(&self) -> bool {
        !self.is_dir()
    }

    fn kind(&self) -> FileType {
        if self.is_dir() {
            FileType::Directory
        } else {
            FileType::RegularFile
        }
    }
}

#[derive(Debug)]
struct Dir {
    name: OsString,
    children: BTreeMap<OsString, Entry>,
    mode: mode_t,
}

#[derive(Debug)]
struct File {
    name: OsString,
    content: BytesMut,
    mode: mode_t,
}

#[derive(Debug)]
struct InnerFs {
    root: Entry,
}

#[derive(Debug)]
struct Fs(RwLock<InnerFs>);

impl Fs {
    pub fn new() -> Self {
        Self(RwLock::new(InnerFs {
            root: Entry::Dir(Dir {
                name: OsString::from("/"),
                children: Default::default(),
                mode: 0o755,
            }),
        }))
    }
}

impl Default for Fs {
    fn default() -> Self {
        Self::new()
    }
}

impl PathFilesystem for Fs {
    type DirEntryStream<'a>
        = Empty<Result<DirectoryEntry>>
    where
        Self: 'a;

    #[instrument(skip(self), err(Debug))]
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    #[instrument(skip(self))]
    async fn destroy(&self, _req: Request) {}

    #[instrument(skip(self), err(Debug))]
    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        let parent = parent.to_string_lossy();
        let name = name.to_string_lossy();
        let mut paths = split_path(&parent);
        paths.push(name.as_ref());

        let mut entry = &self.0.read().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        Ok(ReplyEntry {
            ttl: TTL,
            attr: entry.attr(),
        })
    }

    #[instrument(skip(self))]
    async fn forget(&self, _req: Request, _parent: &OsStr, _nlookup: u64) {}

    #[instrument(skip(self), err(Debug))]
    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();

        debug!("get attr path {}", path);

        let paths = split_path(&path);

        let mut entry = &self.0.read().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        Ok(ReplyAttr {
            ttl: TTL,
            attr: entry.attr(),
        })
    }

    #[instrument(skip(self), err(Debug))]
    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        Ok(ReplyAttr {
            ttl: TTL,
            attr: entry.set_attr(set_attr),
        })
    }

    #[instrument(skip(self), err(Debug))]
    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        let path = parent.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if let Entry::Dir(dir) = entry {
            if dir.children.contains_key(name) {
                return Err(Errno::new_exist());
            }

            let entry = Entry::Dir(Dir {
                name: name.to_owned(),
                children: Default::default(),
                mode: mode as mode_t,
            });
            let attr = entry.attr();

            dir.children.insert(name.to_owned(), entry);

            Ok(ReplyEntry { ttl: TTL, attr })
        } else {
            Err(Errno::new_is_not_dir())
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let path = parent.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if let Entry::Dir(dir) = entry {
            if dir
                .children
                .get(name)
                .ok_or_else(Errno::new_not_exist)?
                .is_dir()
            {
                return Err(Errno::new_is_dir());
            }

            dir.children.remove(name);

            Ok(())
        } else {
            Err(Errno::new_is_not_dir())
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let path = parent.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if let Entry::Dir(dir) = entry {
            let child_dir = dir.children.get(name).ok_or_else(Errno::new_not_exist)?;
            if let Entry::Dir(child_dir) = child_dir {
                if !child_dir.children.is_empty() {
                    return Err(Errno::from(libc::ENOTEMPTY));
                }
            } else {
                return Err(Errno::new_is_not_dir());
            }

            dir.children.remove(name);

            Ok(())
        } else {
            Err(Errno::new_is_not_dir())
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        let origin_parent = origin_parent.to_string_lossy();
        let origin_parent_paths = split_path(&origin_parent);

        let inner_fs = &mut *self.0.write().await;
        let mut origin_parent_entry = &inner_fs.root;

        for path in &origin_parent_paths {
            if let Entry::Dir(dir) = origin_parent_entry {
                origin_parent_entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if origin_parent_entry.is_file() {
            return Err(Errno::new_is_not_dir());
        }

        let mut parent_entry = &inner_fs.root;

        let parent = parent.to_string_lossy();
        let parent_paths = split_path(&parent);

        for path in &parent_paths {
            if let Entry::Dir(dir) = parent_entry {
                parent_entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if parent_entry.is_file() {
            return Err(Errno::new_is_not_dir());
        }

        let mut origin_parent_entry = &mut inner_fs.root;

        for path in origin_parent_paths {
            if let Entry::Dir(dir) = origin_parent_entry {
                origin_parent_entry = dir.children.get_mut(OsStr::new(path)).unwrap();
            } else {
                unreachable!()
            }
        }

        let entry = if let Entry::Dir(dir) = origin_parent_entry {
            dir.children
                .remove(origin_name)
                .ok_or_else(Errno::new_not_exist)?
        } else {
            return Err(Errno::new_is_not_dir());
        };

        let mut parent_entry = &mut inner_fs.root;

        for path in parent_paths {
            if let Entry::Dir(dir) = parent_entry {
                parent_entry = dir.children.get_mut(OsStr::new(path)).unwrap();
            } else {
                unreachable!()
            }
        }

        if let Entry::Dir(dir) = parent_entry {
            dir.children.insert(name.to_owned(), entry);
        } else {
            unreachable!()
        }

        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        let path = path.to_string_lossy();
        let paths = split_path(&path);

        debug!("open path {}", path);

        let mut entry = &self.0.read().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if entry.is_dir() {
            Err(Errno::new_is_dir())
        } else {
            Ok(ReplyOpen { fh: 0, flags })
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let paths = split_path(&path);

        debug!("read path {}", path);

        let mut entry = &self.0.read().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        let file = if let Entry::File(file) = entry {
            file
        } else {
            return Err(Errno::new_is_dir());
        };

        let mut cursor = Cursor::new(&file.content);
        cursor.set_position(offset);

        let size = cursor.remaining().min(size as _);

        let mut data = BytesMut::with_capacity(size);
        // safety
        unsafe {
            data.set_len(size);
        }

        cursor.read_exact(&mut data).unwrap();

        Ok(ReplyData { data: data.into() })
    }

    #[instrument(skip(self), err(Debug))]
    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let paths = split_path(&path);

        debug!("write path {}, paths {:?}", path, paths);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        let file = if let Entry::File(file) = entry {
            file
        } else {
            return Err(Errno::new_is_dir());
        };

        let offset = offset as usize;

        if offset < file.content.len() {
            let mut content = &mut file.content.as_mut()[offset..];

            if content.len() >= data.len() {
                content.write_all(data).unwrap();
            } else {
                content.write_all(&data[..content.len()]).unwrap();
                let written = content.len();

                file.content.put(&data[written..]);
            }
        } else {
            file.content.resize(offset, 0);
            file.content.put(data);
        }

        Ok(ReplyWrite {
            written: data.len() as _,
        })
    }

    #[instrument(skip(self), err(Debug))]
    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn fsync(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        let path = parent.to_string_lossy();
        let paths = split_path(&path);

        debug!("create parent path {}, name {:?}", path, name);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if let Entry::Dir(dir) = entry {
            if dir.children.contains_key(name) {
                return Err(Errno::new_exist());
            }

            let entry = Entry::File(File {
                name: name.to_owned(),
                content: Default::default(),
                mode: mode as mode_t,
            });
            let attr = entry.attr();

            dir.children.insert(name.to_owned(), entry);

            Ok(ReplyCreated {
                ttl: TTL,
                attr,
                generation: 0,
                fh: 0,
                flags: 0,
            })
        } else {
            Err(Errno::new_is_not_dir())
        }
    }

    #[instrument(skip(self))]
    async fn batch_forget(&self, _req: Request, _paths: &[&OsStr]) {}

    // Not supported by fusefs(5) as of FreeBSD 13.0
    #[cfg(target_os = "linux")]
    #[instrument(skip(self), err(Debug))]
    async fn fallocate(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        use std::os::raw::c_int;

        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &mut self.0.write().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get_mut(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        let file = if let Entry::File(file) = entry {
            file
        } else {
            return Err(Errno::new_is_dir());
        };

        let offset = offset as usize;
        let length = length as usize;

        match mode as c_int {
            0 => {
                if offset + length > file.content.len() {
                    file.content.resize(offset + length, 0);
                }

                Ok(())
            }

            libc::FALLOC_FL_KEEP_SIZE => {
                if offset + length > file.content.len() {
                    file.content.reserve(offset + length - file.content.len());
                }

                Ok(())
            }

            _ => Err(Errno::from(libc::EOPNOTSUPP)),
        }
    }

    type DirEntryPlusStream<'a>
        = Iter<IntoIter<Result<DirectoryEntryPlus>>>
    where
        Self: 'a;

    #[instrument(skip(self), err(Debug))]
    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let path = parent.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &self.0.read().await.root;
        let mut parent = entry;

        for path in paths {
            parent = entry;

            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        if let Entry::Dir(dir) = entry {
            let pre_children = vec![
                (FileType::Directory, OsString::from("."), entry.attr(), 1),
                (FileType::Directory, OsString::from(".."), parent.attr(), 2),
            ];

            let pre_children = stream::iter(pre_children);

            let children =
                pre_children
                    .chain(stream::iter(dir.children.iter()).enumerate().map(
                        |(i, (name, entry))| {
                            let kind = entry.kind();
                            let name = name.to_owned();
                            let attr = entry.attr();

                            (kind, name, attr, i as i64 + 3)
                        },
                    ))
                    .map(|(kind, name, attr, offset)| DirectoryEntryPlus {
                        kind,
                        name,
                        offset,
                        attr,
                        entry_ttl: TTL,
                        attr_ttl: TTL,
                    })
                    .skip(offset as _)
                    .map(Ok)
                    .collect::<Vec<_>>()
                    .await;

            Ok(ReplyDirectoryPlus {
                entries: stream::iter(children),
            })
        } else {
            Err(Errno::new_is_not_dir())
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn rename2(
        &self,
        req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.rename(req, origin_parent, origin_name, parent, name)
            .await
    }

    #[instrument(skip(self), err(Debug))]
    async fn lseek(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        let path = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let paths = split_path(&path);

        let mut entry = &self.0.read().await.root;

        for path in paths {
            if let Entry::Dir(dir) = entry {
                entry = dir
                    .children
                    .get(OsStr::new(path))
                    .ok_or_else(Errno::new_not_exist)?;
            } else {
                return Err(Errno::new_is_not_dir());
            }
        }

        let file = if let Entry::File(file) = entry {
            file
        } else {
            return Err(Errno::new_is_dir());
        };

        let whence = whence as i32;

        let offset = if whence == libc::SEEK_CUR || whence == libc::SEEK_SET {
            offset
        } else if whence == libc::SEEK_END {
            let size = file.content.len();

            if size >= offset as _ {
                size as u64 - offset
            } else {
                0
            }
        } else {
            return Err(libc::EINVAL.into());
        };

        Ok(ReplyLSeek { offset })
    }

    #[instrument(skip(self), err(Debug))]
    async fn copy_file_range(
        &self,
        req: Request,
        from_path: Option<&OsStr>,
        fh_in: u64,
        offset_in: u64,
        to_path: Option<&OsStr>,
        fh_out: u64,
        offset_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        let data = self
            .read(req, from_path, fh_in, offset_in, length as _)
            .await?;

        // write_flags set to 0 because we don't care it in this example implement
        let ReplyWrite { written } = self
            .write(req, to_path, fh_out, offset_out, &data.data, 0, flags as _)
            .await?;

        Ok(ReplyCopyFileRange {
            copied: u64::from(written),
        })
    }

    #[doc = " read symbolic link."]
    #[instrument(skip(self), err(Debug))]
    fn readlink(
        &self,
        req: Request,
        path: &OsStr,
    ) -> impl ::core::future::Future<Output = Result<ReplyData>> + Send {
        async move {
            {
                error!("Not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " create a symbolic link."]
    #[instrument(skip(self), err(Debug))]
    fn symlink(
        &self,
        req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> impl ::core::future::Future<Output = Result<ReplyEntry>> + Send {
        async move {
            {
                error!("Not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " create file node. Create a regular file, character device, block device, fifo or socket"]
    #[doc = " node. When creating file, most cases user only need to implement"]
    #[doc = " [`create`][PathFilesystem::create]."]
    #[instrument(skip(self), err(Debug))]
    fn mknod(
        &self,
        req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> impl ::core::future::Future<Output = Result<ReplyEntry>> + Send {
        async move {
            {
                error!("Not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " create a hard link."]
    #[instrument(skip(self), err(Debug))]
    fn link(
        &self,
        req: Request,
        path: &OsStr,
        new_parent: &OsStr,
        new_name: &OsStr,
    ) -> impl ::core::future::Future<Output = Result<ReplyEntry>> + Send {
        async move {
            {
                error!("Not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " get filesystem statistics."]
    #[instrument(skip(self), err(Debug))]
    fn statfs(
        &self,
        _req: Request,
        _path: &OsStr,
    ) -> impl ::core::future::Future<Output = Result<ReplyStatFs>> + Send {
        async move {
            // For a memory filesystem, we can define some reasonable values
            // These values can be adjusted based on your needs
            let blocks = 1024 * 1024; // 1M blocks
            let bfree = 1024 * 1024; // All blocks are free
            let bavail = 1024 * 1024; // All blocks are available
            let files = 10000; // Max number of files
            let ffree = 10000; // All inodes are free
            let bsize = 4096; // Block size (4KB)
            let namelen = 255; // Max filename length
            let frsize = 4096; // Fragment size

            Ok(ReplyStatFs {
                blocks,
                bfree,
                bavail,
                files,
                ffree,
                bsize,
                namelen,
                frsize,
            })
        }
    }

    #[doc = " set an extended attribute."]
    #[instrument(skip(self), err(Debug))]
    fn setxattr(
        &self,
        req: Request,
        path: &OsStr,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("setxattr not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " get an extended attribute. If size is too small, use [`ReplyXAttr::Size`] to return correct"]
    #[doc = " size. If size is enough, use [`ReplyXAttr::Data`] to send it, or return error."]
    #[instrument(skip(self), err(Debug))]
    fn getxattr(
        &self,
        req: Request,
        path: &OsStr,
        name: &OsStr,
        size: u32,
    ) -> impl ::core::future::Future<Output = Result<ReplyXAttr>> + Send {
        async move {
            {
                error!("getxattr not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " list extended attribute names. If size is too small, use [`ReplyXAttr::Size`] to return"]
    #[doc = " correct size. If size is enough, use [`ReplyXAttr::Data`] to send it, or return error."]
    #[instrument(skip(self), err(Debug))]
    fn listxattr(
        &self,
        req: Request,
        path: &OsStr,
        size: u32,
    ) -> impl ::core::future::Future<Output = Result<ReplyXAttr>> + Send {
        async move {
            {
                error!("listxattr not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " remove an extended attribute."]
    #[instrument(skip(self), err(Debug))]
    fn removexattr(
        &self,
        req: Request,
        path: &OsStr,
        name: &OsStr,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("removexattr not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " open a directory. Filesystem may store an arbitrary file handle (pointer, index, etc) in"]
    #[doc = " `fh`, and use this in other all other directory stream operations"]
    #[doc = " ([`readdir`][PathFilesystem::readdir], [`releasedir`][PathFilesystem::releasedir],"]
    #[doc = " [`fsyncdir`][PathFilesystem::fsyncdir]). Filesystem may also implement stateless directory"]
    #[doc = " I/O and not store anything in `fh`.  A file system need not implement this method if it"]
    #[doc = " sets [`MountOptions::no_open_dir_support`][crate::MountOptions::no_open_dir_support] and if"]
    #[doc = " the kernel supports `FUSE_NO_OPENDIR_SUPPORT`."]
    #[instrument(skip(self), err(Debug))]
    fn opendir(
        &self,
        _req: Request,
        path: &OsStr,
        flags: u32,
    ) -> impl ::core::future::Future<Output = Result<ReplyOpen>> + Send {
        async move {
            let path = path.to_string_lossy();
            let paths = split_path(&path);

            debug!("opendir path {}", path);

            let mut entry = &self.0.read().await.root;

            for path in paths {
                if let Entry::Dir(dir) = entry {
                    entry = dir
                        .children
                        .get(OsStr::new(path))
                        .ok_or_else(Errno::new_not_exist)?;
                } else {
                    return Err(Errno::new_is_not_dir());
                }
            }
            if !entry.is_dir() {
                return Err(Errno::new_is_not_dir());
            }

            Ok(ReplyOpen { fh: 0, flags })
        }
    }

    #[doc = " read directory. `offset` is used to track the offset of the directory entries. `fh` will"]
    #[doc = " contain the value set by the [`opendir`][PathFilesystem::opendir] method, or will be"]
    #[doc = " undefined if the [`opendir`][PathFilesystem::opendir] method didn\'t set any value."]
    #[instrument(skip(self), err(Debug))]
    fn readdir<'a>(
        &'a self,
        req: Request,
        path: &'a OsStr,
        fh: u64,
        offset: i64,
    ) -> impl ::core::future::Future<Output = Result<ReplyDirectory<Self::DirEntryStream<'a>>>> + Send
    {
        async move {
            {
                // error!("readdir not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " release an open directory. For every [`opendir`][PathFilesystem::opendir] call there will"]
    #[doc = " be exactly one `releasedir` call. `fh` will contain the value set by the"]
    #[doc = " [`opendir`][PathFilesystem::opendir] method, or will be undefined if the"]
    #[doc = " [`opendir`][PathFilesystem::opendir] method didn\'t set any value."]
    #[instrument(skip(self), err(Debug))]
    fn releasedir(
        &self,
        req: Request,
        path: &OsStr,
        fh: u64,
        flags: u32,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                Ok(())
            }
        }
    }

    #[doc = " synchronize directory contents. If the `datasync` is true, then only the directory contents"]
    #[doc = " should be flushed, not the metadata. `fh` will contain the value set by the"]
    #[doc = " [`opendir`][PathFilesystem::opendir] method, or will be undefined if the"]
    #[doc = " [`opendir`][PathFilesystem::opendir] method didn\'t set any value."]
    #[instrument(skip(self), err(Debug))]
    fn fsyncdir(
        &self,
        req: Request,
        path: &OsStr,
        fh: u64,
        datasync: bool,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("fsyncdir not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " handle interrupt. When a operation is interrupted, an interrupt request will send to fuse"]
    #[doc = " server with the unique id of the operation."]
    #[instrument(skip(self), err(Debug))]
    fn interrupt(
        &self,
        req: Request,
        unique: u64,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("interrupt not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " map block index within file to block index within device."]
    #[doc = ""]
    #[doc = " # Notes:"]
    #[doc = ""]
    #[doc = " This may not works because currently this crate doesn\'t support fuseblk mode yet."]
    #[instrument(skip(self), err(Debug))]
    fn bmap(
        &self,
        req: Request,
        path: &OsStr,
        block_size: u32,
        idx: u64,
    ) -> impl ::core::future::Future<Output = Result<ReplyBmap>> + Send {
        async move {
            {
                error!("bmap not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " poll for IO readiness events."]
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self), err(Debug))]
    fn poll(
        &self,
        req: Request,
        path: Option<&OsStr>,
        fh: u64,
        kn: Option<u64>,
        flags: u32,
        envents: u32,
        notify: &Notify,
    ) -> impl ::core::future::Future<Output = Result<ReplyPoll>> + Send {
        async move {
            {
                error!("poll not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " receive notify reply from kernel."]
    #[instrument(skip(self), err(Debug))]
    fn notify_reply(
        &self,
        req: Request,
        path: &OsStr,
        offset: u64,
        data: Bytes,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("notify_reply not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }

    #[doc = " allocate space for an open file. This function ensures that required space is allocated for"]
    #[doc = " specified file."]
    #[doc = ""]
    #[doc = " # Notes:"]
    #[doc = ""]
    #[doc = " more information about `fallocate`, please see **`man 2 fallocate`**"]
    #[instrument(skip(self), err(Debug))]
    fn fallocate(
        &self,
        req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> impl ::core::future::Future<Output = Result<()>> + Send {
        async move {
            {
                error!("fallocate not implemented");
                Err(libc::ENOSYS.into())
            }
        }
    }
}

fn split_path(path: &str) -> Vec<&str> {
    if path == "/" {
        vec![]
    } else {
        path.split(SEPARATOR).skip(1).collect()
    }
}

fn log_init() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace,fuse3=debug"));

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::EXIT,
        )
        .init();
}

#[tokio::main]
async fn main() {
    log_init();

    let args = env::args_os().skip(1).take(1).collect::<Vec<_>>();

    let a = OsString::from("./fuse_example");
    let mount_path = args.first().or(Some(&a));

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut mount_options = MountOptions::default();
    // .allow_other(true)
    mount_options.force_readdir_plus(true).uid(uid).gid(gid);

    let mount_path = mount_path.expect("no mount point specified");
    let not_unprivileged = env::var("NOT_UNPRIVILEGED").ok().as_deref() == Some("1");

    let mut mount_handle = if !not_unprivileged {
        Session::new(mount_options)
            .mount_with_unprivileged(Fs::default(), mount_path)
            .await
            .unwrap()
    } else {
        Session::new(mount_options)
            .mount(Fs::default(), mount_path)
            .await
            .unwrap()
    };

    let handle = &mut mount_handle;

    tokio::select! {
        res = handle => res.unwrap(),
        _ = signal::ctrl_c() => {
            debug!("ctrl_c");
            mount_handle.unmount().await.unwrap();
            debug!("Umount success");
        }
    }
}
