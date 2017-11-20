#include "filesystem.h"

// Initialization

int fs_init() {
	return 0;
}

int fs_destroy() {
	return 0;
}

// Paths
int fs_translate(const char *authority, const char *path, char *fspath) {
	return 0;
}

// Directories

DIR *fs_opendir(const char *path) {
	return 0;
}

struct dirent *fs_readdir(DIR *dirp) {
	return 0;
}

int fs_closedir(DIR *dirp) {
	return 0;
}

int fs_mkdir(const char *path, mode_t mode) {
	return 0;
}

int fs_rmdir(const char *path) {
	return 0;
}

// Files

int fs_open(const char *path, int oflag, ...) {
	return 0;
}

int fs_close(int fildes) {
	return 0;
}

int fs_unlink(const char *path) {
	return 0;
}

ssize_t fs_read(int fildes, void *buf, size_t nbyte) {
	return 0;
}

ssize_t fs_write(int fildes, const void *buf, size_t nbyte) {
	return 0;
}

int fs_stat(const char *path, struct stat *buf) {
	return 0;
}

off_t fs_lseek(int fildes, off_t offset, int whence) {
	return 0;
}

// Distribution

int fs_replication(const char *path) {
	return 0;
}

int fs_locate(const char *path, char ***urls) {
	return 0;
}

int fs_rename(const char *src, const char *dst) {
	return 0;
}

// Change properties

int fs_chmod(const char *path, mode_t permission) {
	return 0;
}

int fs_chown(const char *path, uid_t uid, gid_t gid) {
	return 0;
}
