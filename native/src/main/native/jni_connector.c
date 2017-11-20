#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pwd.h>
#include <grp.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>

#include "fs/filesystem.h"

#define GROUPNAME_MAX 64
#define USERNAME_MAX 64
#define ERR_MAX 1024

// Class name
#define STRING_NAME "java/lang/String"
#define IOEXCEPTION_NAME "java/io/IOException"
#define FILENOTFOUNDEXCEPTION_NAME "java/io/FileNotFoundException"
#define FILEALREADYEXISTSEXCEPTION_NAME "org/apache/hadoop/fs/FileAlreadyExistsException"
#define URI_NAME "java/net/URI"
#define PATH_NAME "org/apache/hadoop/fs/Path"
#define FILESTATUS_NAME "org/apache/hadoop/fs/FileStatus"
#define FSPERMISSION_NAME "org/apache/hadoop/fs/permission/FsPermission"
#define ARRAYLIST_NAME "java/util/ArrayList"
#define BLOCKLOCATION_NAME "org/apache/hadoop/fs/BlockLocation"
#define GENERICINPUTSTREAM_NAME "org/apache/hadoop/fs/connector/generic/stream/GenericInputStream"
#define GENERICOUTPUTSTREAM_NAME "org/apache/hadoop/fs/connector/generic/stream/GenericOutputStream"

// Class definition
static jclass String;
static jclass IOException;
static jclass FileNotFoundException;
static jclass FileAlreadyExistsException;
static jclass URI;
static jclass Path;
static jclass FileStatus;
static jclass FsPermission;
static jclass ArrayList;
static jclass BlockLocation;
static jclass GenericInputStream;
static jclass GenericOutputStream;

// Method definition
static jmethodID URI_init;
static jmethodID URI_getAuthority;
static jmethodID URI_getScheme;
static jmethodID URI_getHost;
static jmethodID Path_init1;
static jmethodID Path_init2;
static jmethodID Path_getPathWithoutSchemeAndAuthority;
static jmethodID Path_toString;
static jmethodID Path_toUri;
static jmethodID FileStatus_init;
static jmethodID FileStatus_getBlockSize;
static jmethodID FileStatus_getLen;
static jmethodID FileStatus_getPath;
static jmethodID FileStatus_getReplication;
static jmethodID FileStatus_isFile;
static jmethodID FileStatus_isDirectory;
static jmethodID FsPermission_init;
static jmethodID ArrayList_init;
static jmethodID ArrayList_add;
static jmethodID BlockLocation_init;

// Field definition
static jfieldID GenericInputStream_fd;
static jfieldID GenericOutputStream_fd;
static jfieldID GenericOutputStream_permission;
static jfieldID GenericOutputStream_overwrite;
static jfieldID GenericOutputStream_append;

//    ###    ##     ## ##     ## #### ##       ####    ###    ########  ##    ##
//   ## ##   ##     ##  ##   ##   ##  ##        ##    ## ##   ##     ##  ##  ##
//  ##   ##  ##     ##   ## ##    ##  ##        ##   ##   ##  ##     ##   ####
// ##     ## ##     ##    ###     ##  ##        ##  ##     ## ########     ##
// ######### ##     ##   ## ##    ##  ##        ##  ######### ##   ##      ##
// ##     ## ##     ##  ##   ##   ##  ##        ##  ##     ## ##    ##     ##
// ##     ##  #######  ##     ## #### ######## #### ##     ## ##     ##    ##

int search_ids(JNIEnv *env) {

	// Search for all required classes

	// String
	String = (*env)->NewGlobalRef(env, (*env)->FindClass(env, STRING_NAME));
	if(!String) return -1;
	// IOException
	IOException = (*env)->NewGlobalRef(env, (*env)->FindClass(env, IOEXCEPTION_NAME));
	if(!IOException) return -1;
	// FileNotFoundException
	FileNotFoundException = (*env)->NewGlobalRef(env, (*env)->FindClass(env, FILENOTFOUNDEXCEPTION_NAME));
	if(!FileNotFoundException) return -1;
	// FileAlreadyExistsException
	FileAlreadyExistsException = (*env)->NewGlobalRef(env, (*env)->FindClass(env, FILEALREADYEXISTSEXCEPTION_NAME));
	if(!FileAlreadyExistsException) return -1;
	// URI
	URI = (*env)->NewGlobalRef(env, (*env)->FindClass(env, URI_NAME));
	if(!URI) return -1;
	// Path
	Path = (*env)->NewGlobalRef(env, (*env)->FindClass(env, PATH_NAME));
	if(!Path) return -1;
	// FileStatus
	FileStatus = (*env)->NewGlobalRef(env, (*env)->FindClass(env, FILESTATUS_NAME));
	if(!FileStatus) return -1;
	// FsPermission
	FsPermission = (*env)->NewGlobalRef(env, (*env)->FindClass(env, FSPERMISSION_NAME));
	if(!FsPermission) return -1;
	// ArrayList
	ArrayList = (*env)->NewGlobalRef(env, (*env)->FindClass(env, ARRAYLIST_NAME));
	if(!ArrayList) return -1;
	// BlockLocation
	BlockLocation = (*env)->NewGlobalRef(env, (*env)->FindClass(env, BLOCKLOCATION_NAME));
	if(!BlockLocation) return -1;
	// GenericInputStream
	GenericInputStream = (*env)->NewGlobalRef(env, (*env)->FindClass(env, GENERICINPUTSTREAM_NAME));
	if(!GenericInputStream) return -1;
	// GenericOutputStream
	GenericOutputStream = (*env)->NewGlobalRef(env, (*env)->FindClass(env, GENERICOUTPUTSTREAM_NAME));
	if(!GenericOutputStream) return -1;

	// Search for all required method IDs

	// URI: (Constructor) URI(String str)
	URI_init = (*env)->GetMethodID(env, URI, "<init>", "(Ljava/lang/String;)V");
	if(!URI_init) return -1;
	// URI: String getAuthority()
	URI_getAuthority = (*env)->GetMethodID(env, URI, "getAuthority", "()Ljava/lang/String;");
	if(!URI_getAuthority) return -1;
	// URI: String getScheme()
	URI_getScheme = (*env)->GetMethodID(env, URI, "getScheme", "()Ljava/lang/String;");
	if(!URI_getScheme) return -1;
	// URI: String getHost()
	URI_getHost = (*env)->GetMethodID(env, URI, "getHost", "()Ljava/lang/String;");
	if(!URI_getHost) return -1;
	// Path: (Constructor) Path(Path, String)
	Path_init1 = (*env)->GetMethodID(env, Path, "<init>", "(Lorg/apache/hadoop/fs/Path;Ljava/lang/String;)V");
	if(!Path_init1) return -1;
	// Path: (Constructor) Path(String, String, String)
	Path_init2 = (*env)->GetMethodID(env, Path, "<init>", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
	if(!Path_init2) return -1;
	// Path: (Static) Path getPathWithoutSchemeAndAuthority(Path)
	Path_getPathWithoutSchemeAndAuthority = (*env)->GetStaticMethodID(env, Path, "getPathWithoutSchemeAndAuthority", "(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/Path;");
	if(!Path_getPathWithoutSchemeAndAuthority) return -1;
	// Path: String toString()
	Path_toString = (*env)->GetMethodID(env, Path, "toString", "()Ljava/lang/String;");
	if(!Path_toString) return -1;
	// Path: URI toUri()
	Path_toUri = (*env)->GetMethodID(env, Path, "toUri", "()Ljava/net/URI;");
	if(!Path_toUri) return -1;
	// FileStatus: (Constructor) FileStatus(long, boolean, int, long, long, Path)
	FileStatus_init = (*env)->GetMethodID(env, FileStatus, "<init>", "(JZIJJJLorg/apache/hadoop/fs/permission/FsPermission;Ljava/lang/String;Ljava/lang/String;Lorg/apache/hadoop/fs/Path;)V");
	if(!FileStatus_init) return -1;
	// FileStatus: long getBlockSize()
	FileStatus_getBlockSize = (*env)->GetMethodID(env, FileStatus, "getBlockSize", "()J");
	if(!FileStatus_getBlockSize) return -1;
	// FileStatus: long getLen()
	FileStatus_getLen = (*env)->GetMethodID(env, FileStatus, "getLen", "()J");
	if(!FileStatus_getLen) return -1;
	// FileStatus: Path getPath()
	FileStatus_getPath = (*env)->GetMethodID(env, FileStatus, "getPath", "()Lorg/apache/hadoop/fs/Path;");
	if(!FileStatus_getPath) return -1;
	// FileStatus: short getReplication()
	FileStatus_getReplication = (*env)->GetMethodID(env, FileStatus, "getReplication", "()S");
	if(!FileStatus_getReplication) return -1;
	// FileStatus: boolean isFile()
	FileStatus_isFile = (*env)->GetMethodID(env, FileStatus, "isFile", "()Z");
	if(!FileStatus_isFile) return -1;
	// FileStatus: boolean isDirectory()
	FileStatus_isDirectory = (*env)->GetMethodID(env, FileStatus, "isDirectory", "()Z");
	if(!FileStatus_isDirectory) return -1;
	// FsPermission: (Constructor) FsPermission(short)
	FsPermission_init = (*env)->GetMethodID(env, FsPermission, "<init>", "(S)V");
	if(!FsPermission_init) return -1;
	// ArrayList: (Constructor) ArrayList()
	ArrayList_init = (*env)->GetMethodID(env, ArrayList, "<init>", "()V");
	if(!ArrayList_init) return -1;
	// ArrayList: boolean add(Object)
	ArrayList_add = (*env)->GetMethodID(env, ArrayList, "add", "(Ljava/lang/Object;)Z");
	if(!ArrayList_add) return -1;
	// BlockLocation: (Constructor) BlockLocation(String[] names, String[] hosts, long offset, long length)
	BlockLocation_init = (*env)->GetMethodID(env, BlockLocation, "<init>", "([Ljava/lang/String;[Ljava/lang/String;JJ)V");
	if(!BlockLocation_init) return -1;

	// Search for all required field IDs

	// GenericInputStream: fd
	GenericInputStream_fd = (*env)->GetFieldID(env, GenericInputStream, "fd", "I");
	if(!GenericInputStream_fd) return -1;
	// GenericOutputStream: fd
	GenericOutputStream_fd = (*env)->GetFieldID(env, GenericOutputStream, "fd", "I");
	if(!GenericOutputStream_fd) return -1;
	// GenericOutputStream: permission
	GenericOutputStream_permission = (*env)->GetFieldID(env, GenericOutputStream, "permission", "S");
	if(!GenericOutputStream_permission) return -1;
	// GenericOutputStream: overwrite
	GenericOutputStream_overwrite = (*env)->GetFieldID(env, GenericOutputStream, "overwrite", "Z");
	if(!GenericOutputStream_overwrite) return -1;
	// GenericOutputStream: append
	GenericOutputStream_append = (*env)->GetFieldID(env, GenericOutputStream, "append", "Z");
	if(!GenericOutputStream_append) return -1;

	return 0;
}

void destroy_ids(JNIEnv *env) {

	// Remove all global references to classes

	// String
	(*env)->DeleteGlobalRef(env, String);
	// IOException
	(*env)->DeleteGlobalRef(env, IOException);
	// FileNotFoundException
	(*env)->DeleteGlobalRef(env, FileNotFoundException);
	// FileAlreadyExistsException
	(*env)->DeleteGlobalRef(env, FileAlreadyExistsException);
	// URI
	(*env)->DeleteGlobalRef(env, URI);
	// Path
	(*env)->DeleteGlobalRef(env, Path);
	// FileStatus
	(*env)->DeleteGlobalRef(env, FileStatus);
	// FsPermission
	(*env)->DeleteGlobalRef(env, FsPermission);
	// ArrayList
	(*env)->DeleteGlobalRef(env, ArrayList);
	// BlockLocation
	(*env)->DeleteGlobalRef(env, BlockLocation);
	// GenericInputStream
	(*env)->DeleteGlobalRef(env, GenericInputStream);
	// GenericOutputStream
	(*env)->DeleteGlobalRef(env, GenericOutputStream);

	return;
}

int remove_directory(const char *path) {
	DIR *d;
	size_t path_len;
	int r = -1;

	path_len = strlen(path);
	d = fs_opendir(path);
	if (d) {
		struct dirent *p;

		r = 0;

		while (!r && (p=fs_readdir(d))){
			int r2 = -1;
			char *buf;
			size_t len;

			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}
			len = path_len + strlen(p->d_name) + 2;
			buf = malloc(len);
			if (buf) {
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path, p->d_name);
				if (!fs_stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = remove_directory(buf);
					}
					else {
						r2 = fs_unlink(buf);
					}
				}
				free(buf);
			}
			r = r2;
		}
		fs_closedir(d);
	}
	if (!r) {
		r = fs_rmdir(path);
	}
	return r;
}

int parseString(JNIEnv *env, const jstring jstr, char* str, int length) {
	const char* tmp;

	// Convert string to char array
	tmp = (*env)->GetStringUTFChars(env, jstr, 0);

	// Check path length
	if(strlen(tmp) >= length) {

		// Release memory for char array
		(*env)->ReleaseStringUTFChars(env, jstr, tmp);
		return -1;
	}

	// Copy char array to result
	strcpy(str, tmp);

	// Release memory for char array
	(*env)->ReleaseStringUTFChars(env, jstr, tmp);

	return 0;
}

int parsePath(JNIEnv *env, const jobject jpath, char* path) {
	jstring jstr;

	// Convert path to string
	jstr = (*env)->CallObjectMethod(env, jpath, Path_toString);

	// Convert string to char array
	if(parseString(env, jstr, path, PATH_MAX)) {
		return -1;
	}

	return 0;
}

int translatePath(JNIEnv *env, const jobject jpath, char* path) {
	char rpath[PATH_MAX], authority[PATH_MAX];
	jobject jpathnouri, uri, jauthority;

	// Remove URI from path
	jpathnouri = (*env)->CallStaticObjectMethod(env, Path, Path_getPathWithoutSchemeAndAuthority, jpath);

	// Get URI from path
	uri = (*env)->CallObjectMethod(env, jpath, Path_toUri);

	// Get authority from URI
	jauthority = (*env)->CallObjectMethod(env, uri, URI_getAuthority);

	// Convert authority to char array
	if(parseString(env, jauthority, authority, PATH_MAX)) return -1;

	// Convert path without URI to char array
	if(parsePath(env, jpathnouri, rpath)) return -1;

	// Generate actual path from authority and its relative path
	return fs_translate(authority, rpath, path);
}

// ##     ##    ###    #### ##    ##
// ###   ###   ## ##    ##  ###   ##
// #### ####  ##   ##   ##  ####  ##
// ## ### ## ##     ##  ##  ## ## ##
// ##     ## #########  ##  ##  ####
// ##     ## ##     ##  ##  ##   ###
// ##     ## ##     ## #### ##    ##

// [GenericFileSystem] void initConnector() throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_initConnector(JNIEnv *env, jobject obj) {
	char err[ERR_MAX];

	// Search for Java class IDs and method IDs
	if(search_ids(env)) {
		return;
	}

	// Initialize Expand library
	if(fs_init()) {
		sprintf(err, "fs_init: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}
}

// [GenericFileSystem] void destConnector() throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_destConnector(JNIEnv *env, jobject obj) {
	char err[ERR_MAX];

	// Destroy cached Java class IDs and method IDs
	destroy_ids(env);

	// Destroy Expand library
	if(fs_destroy()) {
		sprintf(err, "fs_destroy: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}
}

// [GenericFileSystem] FileStatus getFileStatus0(Path path) throws IOException
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_getFileStatus0(JNIEnv *env, jobject obj, jobject jpath) {
	char path[PATH_MAX], err[ERR_MAX];
	DIR *dir;
	struct stat statbuf;
	struct passwd *pwd;
	struct group *grp;
	uid_t uid = -1;
	gid_t gid = -1;
	mode_t mode = 0;
	jlong size = 0, blksize = 0, modtime = 0, acctime = 0;
	jint res = -1, blkrep = -1;
	jboolean isdir;
	jobject permission;
	jstring owner, group;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return NULL;

	// Stat file or directory through Expand library
	res = fs_stat(path, &statbuf);
	if(res < 0) {
		if(errno == ENOENT) {
			sprintf(err, "fs_stat: %s", strerror(errno));
			(*env)->ThrowNew(env, FileNotFoundException, err);
			return NULL;
		}
		else {
			sprintf(err, "fs_stat: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return NULL;
		}
	}

	// Prepare results
	size = (jlong) statbuf.st_size;
	isdir = S_ISDIR(statbuf.st_mode);
	blkrep = fs_replication(path);
	blksize = (jlong) statbuf.st_blksize;
	modtime = (jlong) statbuf.st_mtime * (jlong) 1000;
	acctime = (jlong) statbuf.st_atime * (jlong) 1000;
	mode = (mode_t) statbuf.st_mode;
	uid = (uid_t) statbuf.st_uid;
	gid = (gid_t) statbuf.st_gid;
	if(blkrep == -1) {
		sprintf(err, "fs_replication: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return NULL;
	}

	// Convert mode (short) to permission (FsPermission)
	permission = (*env)->NewObject(env, FsPermission, FsPermission_init, mode);

	// Convert uid (short) to owner (String)
	pwd = getpwuid(uid);
	if(pwd == NULL) {
		if(errno == 0 || errno == ENOENT || errno == ESRCH || errno == EBADF || errno == EPERM) owner = (*env)->NewStringUTF(env, "unknown");
		else {
			sprintf(err, "getpwuid: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return NULL;
		}
	}
	else owner = (*env)->NewStringUTF(env, pwd->pw_name);

	// Convert gid (short) to group (String)
	grp = getgrgid(gid);
	if(grp == NULL) {
		if(errno == 0 || errno == ENOENT || errno == ESRCH || errno == EBADF || errno == EPERM) group = (*env)->NewStringUTF(env, "unknown");
		else {
			sprintf(err, "getgrgid: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return NULL;
		}
	}
	else group = (*env)->NewStringUTF(env, grp->gr_name);

	return (*env)->NewObject(env, FileStatus, FileStatus_init, size, isdir, blkrep, blksize, modtime, acctime, permission, owner, group, jpath);
}

// [GenericFileSystem] List<Path> getDirEntries0(Path path) throws IOException
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_getDirEntries0(JNIEnv *env, jobject obj, jobject jpath) {
	char path[PATH_MAX], err[ERR_MAX];
	DIR* dp;
	struct dirent *ent;
	jobject ret;
	jsize cnt = 0;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return NULL;

	// Open directory through Expand library (if NULL, path points to file)
	dp = fs_opendir((const char *) path);
	if(!dp) {
		if (errno == ENOTDIR) return NULL;
		else {
			sprintf(err, "fs_opendir: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return NULL;
		}
	}

	// Instantiate ArrayList to store results
	ret = (jobject) (*env)->NewObject(env, ArrayList, ArrayList_init);

	// Read all directory entries through Expand library
	while(ent = fs_readdir(dp)) {
		jstring filename;
		jobject newpath;

		// Ignore self directory and parent directory
		if(!strcmp(".", ent->d_name) || !strcmp("..", ent->d_name)) continue;

		// Convert name char array to path
		filename = (*env)->NewStringUTF(env, ent->d_name);

		// Append entry name to parent path
		newpath = (*env)->NewObject(env, Path, Path_init1, jpath, filename);

		// Add entry to ArrayList object
		(*env)->CallBooleanMethod(env, ret, ArrayList_add, newpath);
	}

	// Close directory through Expand
	fs_closedir(dp);

	return ret;
}

// [GenericFileSystem] boolean mkdirs0(Path path) throws IOException
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_mkdirs0(JNIEnv *env, jobject obj, jobject jpath, jshort permission) {
	struct stat check;
	char path[PATH_MAX], err[ERR_MAX], *pointer;
	int res;
	jsize length;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return JNI_FALSE;

	// Save path length
	length = strlen(path);

	// Root (mount point) is always present
	if(strcmp(path, "/") == 0) return JNI_TRUE;

	// Remove trailing "/" from path if present
	if(path[length-1] == '/') path[length-1] = '\0';

	// Iterate over path char by char
	for(pointer = path + 1; *pointer != '\0'; pointer++) {

		// Once a "/" is found, convert to NULL to fake string ending
		if(*pointer == '/') {
			*pointer = '\0';

			// Get file stats
			res = fs_stat(path, &check);

			if(res == 0 && S_ISDIR(check.st_mode)) {

				// If file exists, and it is a directory, don't make it
				*pointer = '/';
				continue;
			}
			else if (res == 0) {

				// If file exists, but is not a directory, throw IOException
				sprintf(err, "fs_mkdir: '%s' is a FILE", path);
				(*env)->ThrowNew(env, IOException, err);
				return JNI_FALSE;
			}
			else {

				// If file doesn't exist, make directory
				res = fs_mkdir(path, permission);
				if(res < 0) {
					if(errno == EEXIST) {

						// If directory is already there, don't make it
						*pointer = '/';
						continue;
					}
					else {
						sprintf(err, "fs_mkdir: %s", strerror(errno));
						(*env)->ThrowNew(env, IOException, err);
						return JNI_FALSE;
					}
				}
			}

			// Turn '\0' back to '/' so that processing can continue
			*pointer = '/';
		}
	}

	// Get file stats for last entry
	res = fs_stat(path, &check);

	if(res == 0 && S_ISDIR(check.st_mode)) {

		// If file exists, and it is a directory, don't make it (we're done)
		return JNI_TRUE;
	}
	else if(res == 0) {

		// If file exists, but is not a directory, throw IOException
		sprintf(err, "fs_mkdir: '%s' is a FILE", path);
		(*env)->ThrowNew(env, IOException, err);
		return JNI_FALSE;
	}
	else {

		// Make last directory entry through Expand library (full path)
		if(fs_mkdir(path, permission)) {
			sprintf(err, "fs_mkdir: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return JNI_FALSE;
		}
		return JNI_TRUE;
	}
}

// [GenericFileSystem] boolean rename0(Path src, Path dst) throws IOException
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_rename0(JNIEnv *env, jobject obj, jobject jsrc, jobject jdst) {
	char src[PATH_MAX], dst[PATH_MAX], err[ERR_MAX];

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jsrc, src)) return JNI_FALSE;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jdst, dst)) return JNI_FALSE;

	// Rename *source* file or directory to *destination* through Expand library
	if(fs_rename(src, dst)) {
		if(errno == EEXIST) {
			sprintf(err, "fs_rename: %s", strerror(errno));
			(*env)->ThrowNew(env, FileAlreadyExistsException, err);
			return JNI_FALSE;
		}
		else if(errno == ENOENT) {
			sprintf(err, "fs_rename: %s", strerror(errno));
			(*env)->ThrowNew(env, FileNotFoundException, err);
			return JNI_FALSE;
		}
		else {
			sprintf(err, "fs_rename: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return JNI_FALSE;
		}
	}

	return JNI_TRUE;
}

// [GenericFileSystem] boolean delete0(Path path, boolean recursive) throws IOException
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_delete0(JNIEnv *env, jobject obj, jobject jpath, jboolean recursive) {
	char path[PATH_MAX], err[ERR_MAX], *pointer;
	struct stat check;
	jsize length;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return JNI_FALSE;

	// Get file stats
	if(fs_stat(path, &check)) {
			sprintf(err, "fs_stat: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return JNI_FALSE;
	}

	// Behaviour depends on file being a directory or a regular file
	if(S_ISDIR(check.st_mode)) {

		// Recursive operation shall delete directory and all of its contents
		if(recursive) {

			// Run recursive removal function
			if(remove_directory(path)) {
				sprintf(err, "remove_directory: recursive operation failed");
				(*env)->ThrowNew(env, IOException, err);
				return JNI_FALSE;
			}
			else return JNI_TRUE;
		}

		// Non-recursive operation shall delete directory only if empty
		else {

			// Remove directory through Expand library
			if(fs_rmdir(path)) {
				if(errno == ENOENT) {
					sprintf(err, "fs_rmdir: %s", strerror(errno));
					(*env)->ThrowNew(env, FileNotFoundException, err);
					return JNI_FALSE;
				}
				else {
					sprintf(err, "fs_rmdir: %s", strerror(errno));
					(*env)->ThrowNew(env, IOException, err);
					return JNI_FALSE;
				}
			}
			else return JNI_TRUE;
		}
	}
	else {

		// Unlink file through Expand library (if file, recursive flag ignored)
		if(fs_unlink(path)) {
			if(errno == ENOENT) {
				sprintf(err, "fs_unlink: %s", strerror(errno));
				(*env)->ThrowNew(env, FileNotFoundException, err);
				return JNI_FALSE;
			}
			else {
				sprintf(err, "fs_unlink: %s", strerror(errno));
				(*env)->ThrowNew(env, IOException, err);
				return JNI_FALSE;
			}
		}
		else return JNI_TRUE;
	}
}

// [GenericFileSystem] void setPermission0(Path f, short permission) throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_setPermission0(JNIEnv *env, jobject obj, jobject jpath, jshort permission) {
	char path[PATH_MAX], err[ERR_MAX];

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return;

	// Change permission through Expand library
	if(fs_chmod(path, permission)) {
		sprintf(err, "fs_chmod: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	return;
}

// [GenericFileSystem] void setOwner0(Path path, String username, String groupname) throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_setOwner0(JNIEnv *env, jobject obj, jobject jpath, jstring username, jstring groupname) {
	char path[PATH_MAX], owner[USERNAME_MAX], group[GROUPNAME_MAX], err[ERR_MAX];
	struct passwd *pwd;
	struct group *grp;
	uid_t uid;
	gid_t gid;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return;

	// If username is null, then nothing shall be changed
	if(username != NULL) {

		// Convert username string to char array
		if(parseString(env, username, owner, USERNAME_MAX)) return;

		// Convert owner (char array) to uid (short)
		pwd = getpwnam(owner);
		if(pwd == NULL) {
			if(errno == 0 || errno == ENOENT || errno == ESRCH || errno == EBADF || errno == EPERM) {
				sprintf(err, "getpwnam: unknown username");
				(*env)->ThrowNew(env, IOException, err);
				return;
			}
			else {
				sprintf(err, "getpwnam: %s", strerror(errno));
				(*env)->ThrowNew(env, IOException, err);
				return;
			}
		}
		else uid = pwd->pw_uid;
	}
	else uid = -1;

	// If groupname is null, then nothing shall be changed
	if(groupname != NULL) {

		// Convert groupname string to char array
		if(parseString(env, groupname, group, GROUPNAME_MAX)) return;

		// Convert group (char array) to gid (short)
		grp = getgrnam(group);
		if(grp == NULL) {
			if(errno == 0 || errno == ENOENT || errno == ESRCH || errno == EBADF || errno == EPERM) {
				sprintf(err, "getgrnam: unknown groupname");
				(*env)->ThrowNew(env, IOException, err);
				return;
			}
			else {
				sprintf(err, "getgrnam: %s", strerror(errno));
				(*env)->ThrowNew(env, IOException, err);
				return;
			}
		}
		else gid = grp->gr_gid;
	}
	else gid = -1;

	// Change ownership through Expand library
	if(fs_chown(path, uid, gid)) {
		sprintf(err, "fs_chown: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	return;
}

// [GenericFileSystem] BlockLocation[] getFileBlockLocations0(FileStatus file, long start, long len) throws IOException
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_connector_generic_GenericFileSystem_getFileBlockLocations0(JNIEnv *env, jobject obj, jobject file, jlong start, jlong len) {
	char path[PATH_MAX], err[ERR_MAX];
	char*** urls;
	jlong tlen = 0, blksize = 0, nblks = 0, i = 0, fblk = 0, lblk = 0, tblks = 0, z = 0;
	jshort replication = 0, j = 0;
	jobject jpath;
	jobjectArray blockLocations;
	jboolean isFile = JNI_FALSE, isDir = JNI_FALSE;

	// Retrieve all useful data from file object.
	jpath = (*env)->CallObjectMethod(env, file, FileStatus_getPath);
	tlen = (*env)->CallLongMethod(env, file, FileStatus_getLen);
	blksize = (*env)->CallLongMethod(env, file, FileStatus_getBlockSize);
	replication = (*env)->CallShortMethod(env, file, FileStatus_getReplication);
	isFile = (*env)->CallBooleanMethod(env, file, FileStatus_isFile);
	isDir = (*env)->CallBooleanMethod(env, file, FileStatus_isDirectory);

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return JNI_FALSE;

		// Calculate first block (floor)
	fblk = start / blksize;

	// Calculate last block (ceil)
	lblk = tlen / blksize + (tlen % blksize != 0);

	// Calculate number of blocks to locate
	nblks = lblk - fblk;

	// Calculate total number of blocks in file (ceil)
	tblks = len / blksize + (len % blksize != 0);

	// Allocate memory for all blocks
	urls = malloc(nblks * sizeof(char**));

	for(i = 0; i < tblks; i++) {

		// Allocate memory for all replicas of this block
		urls[i] = malloc(replication * sizeof(char*));

		for(j = 0; j < replication; j++) {

			// Allocate memory for the URL storing this replica of the block
			urls[i][j] = malloc(HOST_NAME_MAX * sizeof(char));
		}
	}

	// Retrieve URLs through Expand library
	if(fs_locate(path, urls)) {
		sprintf(err, "fs_locate: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return NULL;
	}

	// Prepare block locations array
	blockLocations = (*env)->NewObjectArray(env, nblks, BlockLocation, NULL);

	// Set block locations array using Expand library results
	for(i = fblk; i < lblk; i++) {
		char split[HOST_NAME_MAX];
		jobjectArray names, hosts;
		jlong offset, length;
		jobject blockLocation;

		// Prepare name array
		names = (*env)->NewObjectArray(env, replication, String, NULL);

		// Prepare host array
		hosts = (*env)->NewObjectArray(env, replication, String, NULL);

		// Set names and hosts using Expand library results
		for(j = 0; j < replication; j++) {
			char debug[HOST_NAME_MAX];
			jstring name, host;
			jobject uri;

			// Parse to URI
			uri = (*env)->NewObject(env, URI, URI_init, (*env)->NewStringUTF(env, urls[i][j]));

			// Get authority from URI
			name = (*env)->CallObjectMethod(env, uri, URI_getAuthority);

			// Get host from URI
			host = (*env)->CallObjectMethod(env, uri, URI_getHost);

			// Add name to array
			(*env)->SetObjectArrayElement(env, names, j, name);

			// Add host to array
			(*env)->SetObjectArrayElement(env, hosts, j, host);
		}

		// Calculate offset from file start
		offset = i * blksize;

		// Calculate length of block
		length = blksize;

		// Create BlockLocation for this block
		blockLocation = (*env)->NewObject(env, BlockLocation, BlockLocation_init, names, hosts, offset, length);

		// Add BlockLocation to array
		(*env)->SetObjectArrayElement(env, blockLocations, z, blockLocation);

		// Next BlockLocation array position
		z++;
	}

	// Free resources
	for(i = 0; i < tblks; i++) {
		for(j = 0; j < replication; j++) {
			free(urls[i][j]);
		}
		free(urls[i]);
	}
	free(urls);

	return blockLocations;
}

// #### ##    ## ########  ##     ## ########
//  ##  ###   ## ##     ## ##     ##    ##
//  ##  ####  ## ##     ## ##     ##    ##
//  ##  ## ## ## ########  ##     ##    ##
//  ##  ##  #### ##        ##     ##    ##
//  ##  ##   ### ##        ##     ##    ##
// #### ##    ## ##         #######     ##

// [GenericInputStream] void open0(Path path) throws FileNotFoundException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericInputStream_open0(JNIEnv *env, jobject obj, jobject jpath) {
	char path[PATH_MAX], err[ERR_MAX];
	int flag = O_RDONLY;
	jint fd = -1;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return;

	// Open file through Expand library
	fd = fs_open(path, flag);
	if(fd < 0) {
		sprintf(err, "fs_open: %s", strerror(errno));
		(*env)->ThrowNew(env, FileNotFoundException, err);
		return;
	}

	// Save fd field to keep value in calling object
	(*env)->SetIntField(env, obj, GenericInputStream_fd, fd);

	return;
}

// [GenericInputStream] int read0() throws IOException
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericInputStream_read0(JNIEnv *env, jobject obj) {
	char err[ERR_MAX];
	unsigned char buffer = 0;
	jint res = -1, fd = -1;

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericInputStream_fd);

	// Read file through Expand library
	res = fs_read(fd, &buffer, 1);
	if(res == 0) return -1; // EOF
	else if(res < 0) {
		sprintf(err, "fs_read: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return -1;
	}
	else return buffer;
}

// [GenericInputStream] int readBytes(byte b[], int off, int len) throws IOException
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericInputStream_readBytes(JNIEnv *env, jobject obj, jbyteArray jbuffer, jint off, jint len) {
	char err[ERR_MAX];
	jint res = -1, fd = -1;
	jbyte buffer[len];

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericInputStream_fd);

	// Read file through Expand library
	res = fs_read(fd, buffer, len);
	if(res == 0) return -1; // EOF
	else if(res < 0) {
		sprintf(err, "fs_read: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return -1;
	}

	// If at least a byte was read, save result array
	(*env)->SetByteArrayRegion(env, jbuffer, off, res, buffer);

	return res;
}

// [GenericInputStream] void seek0(long pos) throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericInputStream_seek0(JNIEnv *env, jobject obj, jlong pos) {
	char err[ERR_MAX];
	jint res = -1, fd = -1;

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericInputStream_fd);

	// Change current file pointer position through Expand library
	res = fs_lseek(fd, pos, SEEK_SET);
	if(res != pos) {
		sprintf(err, "fs_lseek: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	return;
}

// [GenericInputStream] void close0() throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericInputStream_close0(JNIEnv *env, jobject obj) {
	char err[ERR_MAX];
	jint fd = -1;

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericInputStream_fd);

	// Close file through Expand library
	if(fs_close(fd)) {
		sprintf(err, "fs_close: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	// Invalidate fd field in calling object
	(*env)->SetIntField(env, obj, GenericInputStream_fd, -1);

	return;
}

//  #######  ##     ## ######## ########  ##     ## ########
// ##     ## ##     ##    ##    ##     ## ##     ##    ##
// ##     ## ##     ##    ##    ##     ## ##     ##    ##
// ##     ## ##     ##    ##    ########  ##     ##    ##
// ##     ## ##     ##    ##    ##        ##     ##    ##
// ##     ## ##     ##    ##    ##        ##     ##    ##
//  #######   #######     ##    ##         #######     ##

// [GenericOutputStream] void open0(Path path) throws FileNotFoundException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericOutputStream_open0(JNIEnv *env, jobject obj, jobject jpath) {
	char path[PATH_MAX], err[ERR_MAX];
	int flags = O_WRONLY;
	jint fd = -1;
	jshort permission = -1;
	jboolean overwrite = JNI_FALSE, append = JNI_FALSE;

	// Translate Hadoop path to filesystem path
	if(translatePath(env, jpath, path)) return;

	append = (*env)->GetBooleanField(env, obj, GenericOutputStream_append);

	// CREATE operation
	if(!append) {

		// Retrieve permission field from calling object
		permission = (*env)->GetShortField(env, obj, GenericOutputStream_permission);

		// Retrieve overwrite field from calling object
		overwrite = (*env)->GetBooleanField(env, obj, GenericOutputStream_overwrite);

		// Adjust open flags as per overwrite
		if(overwrite) flags |= (O_TRUNC | O_CREAT);
		else flags |= (O_EXCL | O_CREAT);
	}
	// APPEND operation
	else flags |= O_APPEND;

	// Open file through Expand library
	fd = fs_open(path, flags, permission);
	if(fd < 0) {
		sprintf(err, "fs_open: %s", strerror(errno));
		if(overwrite || append) (*env)->ThrowNew(env, FileNotFoundException, err);
		else (*env)->ThrowNew(env, FileAlreadyExistsException, err);
		return;
	}

	// Save fd field to keep value in calling object
	(*env)->SetIntField(env, obj, GenericOutputStream_fd, fd);

	return;
}

// [GenericOutputStream] void write0(byte b) throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericOutputStream_write0(JNIEnv *env, jobject obj, jint jbuffer) {
	char err[ERR_MAX];
	unsigned char buffer = jbuffer;
	jint res = -1, fd = -1;

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericOutputStream_fd);

	// Write file through Expand library
	res = fs_write(fd, &buffer, 1);
	if(res < 1) {
		sprintf(err, "fs_write: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	return;
}

// [GenericOutputStream] void writeBytes(byte b[], int off, int len) throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericOutputStream_writeBytes(JNIEnv *env, jobject obj, jbyteArray jbuffer, jint off, jint len) {
	char err[ERR_MAX];
	jbyte buffer[len];
	jint res = -1, count = 0, left = len, fd = -1;

	// Convert byte array object to byte array
	(*env)->GetByteArrayRegion(env, jbuffer, off, len, buffer);

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericOutputStream_fd);

	// Write file through Expand library
	while(res = fs_write(fd, buffer + count, left)) {
		if(res < 0) {
			sprintf(err, "fs_write: %s", strerror(errno));
			(*env)->ThrowNew(env, IOException, err);
			return;
		}
		count += res;
		left -= res;
	}

	return;
}

// [GenericOutputStream] void close0() throws IOException
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_connector_generic_stream_GenericOutputStream_close0(JNIEnv *env, jobject obj) {
	char err[ERR_MAX];
	jint fd = -1;

	// Retrieve fd field from calling object
	fd = (*env)->GetIntField(env, obj, GenericOutputStream_fd);

	// Close file through Expand library
	if(fs_close(fd)) {
		sprintf(err, "fs_close: %s", strerror(errno));
		(*env)->ThrowNew(env, IOException, err);
		return;
	}

	// Invalidate fd field in calling object
	(*env)->SetIntField(env, obj, GenericOutputStream_fd, -1);

	return;
}
