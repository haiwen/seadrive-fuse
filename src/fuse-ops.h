#ifndef SEADRIVE_FUSE_OPS_H
#define SEADRIVE_FUSE_OPS_H

#if defined __linux__ || defined __APPLE__

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 30
#endif
#include <fuse3/fuse.h>

int seadrive_fuse_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi);

int seadrive_fuse_readdir(const char *path, void *buf,
                          fuse_fill_dir_t filler, off_t offset,
                          struct fuse_file_info *info,
                          enum fuse_readdir_flags flags);

int seadrive_fuse_mknod (const char *path, mode_t mode, dev_t dev);

int seadrive_fuse_mkdir (const char *path, mode_t mode);

int seadrive_fuse_unlink (const char *path);

int seadrive_fuse_rmdir (const char *path);

int seadrive_fuse_rename (const char *oldpath, const char *newpath, unsigned int flags);

int seadrive_fuse_open (const char *path, struct fuse_file_info *info);

int seadrive_fuse_read (const char *path, char *buf, size_t size,
                        off_t offset, struct fuse_file_info *info);

int seadrive_fuse_write (const char *path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *info);

int seadrive_fuse_release (const char *path, struct fuse_file_info *fi);

int seadrive_fuse_truncate (const char *path, off_t length, struct fuse_file_info *fi);

int seadrive_fuse_statfs (const char *path, struct statvfs *buf);

int seadrive_fuse_chmod (const char *path, mode_t mode, struct fuse_file_info *fi);

int seadrive_fuse_utimens (const char *, const struct timespec tv[2], struct fuse_file_info *fi);

int seadrive_fuse_symlink (const char *from, const char *to);

int seadrive_fuse_link (const char *from, const char *to);

int seadrive_fuse_setxattr (const char *path, const char *name, const char *value,
                        size_t size, int flags);

int seadrive_fuse_getxattr (const char *path, const char *name, char *value, size_t size);

int seadrive_fuse_listxattr (const char *path, char *list, size_t size);

int seadrive_fuse_removexattr (const char *path, const char *name);


#endif // __linux__

#endif
