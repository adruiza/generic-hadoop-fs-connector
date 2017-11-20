#include <sys/stat.h>
#include <sys/types.h>
#include <sys/dir.h>

//
// POSIX definitions
//
// All of this definitions are expected to work exactly as the standard function
// set provided by The Open Group in the following URL:
//
// http://pubs.opengroup.org/onlinepubs/9699919799/
//
// ATTENTION
//
// fs_translate, fs_replication and fs_locate are custom definitions that do not
// follow the standard. Refer to them for more information about parameters and
// returned values.
//

// Initialization

int fs_init();

int fs_destroy();

// Paths

/*
 * This function gives the path string expected by the filesystem for an
 * authority and a path. In Expand, for example, xpn://partition1/tmp must be
 * translated to /partition1/tmp. In other filesystems, fs://host:port/path
 * could be translated to /port//host:path, for example. Every filesystem must
 * implement this function even if they don't use authority information at all.
 * PARAM authority A string of form name[:port] as specified in the URI
 * PARAM path A string represeting the path for that authority
 * RETURNS -1 if error, 0 if no error
 */
int fs_translate(const char *authority, const char *path, char *fspath);

// Directories

DIR *fs_opendir(const char *path);

struct dirent *fs_readdir(DIR *dirp);

int fs_closedir(DIR *dirp);

int fs_mkdir(const char *path, mode_t mode);

int fs_rmdir(const char *path);

// Files

int fs_open(const char *path, int oflag, ...);

int fs_close(int fildes);

int fs_unlink(const char *path);

ssize_t fs_read(int fildes, void *buf, size_t nbyte);

ssize_t fs_write(int fildes, const void *buf, size_t nbyte);

int fs_stat(const char *path, struct stat *buf);

off_t fs_lseek(int fildes, off_t offset, int whence);

// Distribution

/*
 * This function retrieves the replication of a file in the filesystem. As some
 * systems may not have a single replication factor for a file but for its
 * blocks, it should be expanded in the future to make it more complex.
 * PARAM path Path of the file for which the replication wants to be retrieved
 * RETURNS -1 if error or the number of replicas for the file if no error
 */
int fs_replication(const char *path);

/*
 * This function retrieves, for each block and its corresponding replicas, the
 * hostname that stores them. In this version, memory allocation is not needed,
 * but in the future it should be provided.
 * PARAM path Path of the file which blocks are to be located
 *       urls Array containing hostname storing block i and replica j
 * RETURNS -1 if error, 0 if no error
 */
int fs_locate(const char *path, char ***urls);

int fs_rename(const char *src, const char *dst);

// Change properties

int fs_chmod(const char *path, mode_t permission);

int fs_chown(const char *path, uid_t uid, gid_t gid);
