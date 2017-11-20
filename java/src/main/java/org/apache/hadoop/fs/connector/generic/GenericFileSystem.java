package org.apache.hadoop.fs.connector.generic;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.net.URI;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.connector.generic.stream.GenericInputStream;
import org.apache.hadoop.fs.connector.generic.stream.GenericOutputStream;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class GenericFileSystem extends FileSystem {

	public final static Log LOG = LogFactory.getLog(GenericFileSystem.class);

	private URI uri;	// Initial URI for FileSystem
	private Path workingDir;	// Current working directory

	public GenericFileSystem() {
		super();
	}

	private Path makeAbsolute(Path f) {

		// If path is null, nothing shall be done
		if(f == null) return null;

		// If path is absolute and scheme & authority are not null, then path is already absolute
		if(f.isAbsolute() && !f.isAbsoluteAndSchemeAuthorityNull()) return f;

		// Append path to current working directory
		return new Path(workingDir, f);
	}

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		LOG.debug("Initializing filesystem");

		super.initialize(uri, conf);
		this.uri = uri;
		this.workingDir = new Path(uri);

		// Load required native library
		System.loadLibrary("generic");

		// Initialize connector (JNI IDs and Expand Library)
		initConnector();

		return;
	}

	@Override
	public void close() throws IOException {
		LOG.debug("Closing filesystem");

		// Destroy connector (JNI Global References and Expand Library)
		destConnector();

		return;
	}

	@Override
	public URI getUri() {
		LOG.debug("Get URI: " + this.uri);

		return this.uri;
	}

	@Override
	public Path getWorkingDirectory() {
		LOG.debug("Get working directory " + this.workingDir);

		return this.workingDir;
	}

	@Override
	public void setWorkingDirectory(Path f) {
		LOG.debug("Set working directory to " + f);

		this.workingDir = makeAbsolute(f);
		return;
	}

	@Override
	public void setOwner(Path f, String username, String groupname) throws IOException {

		// Both username and groupname can't be null
		if(username == null && groupname == null) return;

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Set owner for " + f + " to " + username + " and group to " + groupname);

		// Set ownership
		setOwner0(f, username, groupname);

		return;
	}

	@Override
	public void setPermission(Path f, FsPermission permission) throws IOException {

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Set permissions for " + f + " to " + permission);

		// Apply permissions
		setPermission0(f, permission.toShort());

		return;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		LOG.debug("Get block location for " + file.getPath() + "[start: " + start + ", len: " + len + "]");

		// Check arguments
		if(file == null) {
			return null;
		}

		if (start < 0 || len < 0) {
			throw new IllegalArgumentException("Invalid start or len parameter");
		}

		if (file.getLen() <= start) {
			return new BlockLocation[0];
		}

		return getFileBlockLocations0(file, start, len);
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		GenericInputStream in;
		FileStatus stat;

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Open file " + f);

		// If file doesn't exists, throw exception
		stat = getFileStatus(f);

		// If file is directory, throw exception
		if(stat.isDirectory()) throw new FileNotFoundException("open() cannot open directories");

		// Create stream
		in = new GenericInputStream(f, stat.getLen(), statistics);

		return new FSDataInputStream(in);
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		GenericOutputStream out;
		FileStatus stat;

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Append to file " + f);

		// If file doesn't exists, throw exception
		stat = getFileStatus(f);

		// If file is directory, throw exception
		if(stat.isDirectory()) throw new FileNotFoundException("open() cannot open directories");

		// Create stream
		out = new GenericOutputStream(f, statistics);

		return new FSDataOutputStream(out);
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		GenericOutputStream out;
		Path parent;

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Create file " + f + " with permissions " + permission + " and with overwrite=" + overwrite);

		// If overwrite not set and file exists, throw error
		if(!overwrite) {
			try {
				getFileStatus(f);
				throw new FileAlreadyExistsException();
			}
			catch(FileNotFoundException e) {}
		}

		// Get file container folder
		parent = f.getParent();

		// The folders shall be created with the default permissions
		if(parent != null) mkdirs(parent);

		// Create ConnectorNOutputStream in CREATE mode after creating all required directories
		out = new GenericOutputStream(f, permission, overwrite, statistics);

		return new FSDataOutputStream(out);
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {

		// Compose absolute path for source name and destination name
		src = makeAbsolute(src);
		dst = makeAbsolute(dst);

		LOG.debug("Rename " + src + " to " + dst);

		return rename0(src, dst);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Delete " + f + " with recursive=" + recursive);

		return delete0(f, recursive);
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
		FileStatus parentStatus;

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("List status for path " + f);

		// Get parent's FileStatus
		parentStatus = getFileStatus(f);

		// If file is a directory, get all entries and list their status
		if(parentStatus.isDir()) {
			List<Path> fileNames;
			List<FileStatus> fileStatuses;

			// Get a list of paths corresponding to files in directory
			fileNames = getDirEntries0(f);

			// If directory has no entries, return empty array
			if(fileNames == null || fileNames.size() == 0) return new FileStatus[0];
			else {
				fileStatuses = new ArrayList<FileStatus>();

				// Loop through each file and get its status
				for(int i = 0; i < fileNames.size(); i++) {
					fileStatuses.add(getFileStatus(fileNames.get(i)));
				}

				return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
			}
		}
		else return new FileStatus[0];
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Make all directories to " + f + " with permissions " + permission);

		return mkdirs0(f, permission.toShort());
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {

		// Compose absolute path
		f = makeAbsolute(f);

		LOG.debug("Get file status for " + f);

		return getFileStatus0(f);
	}

	private native synchronized void initConnector() throws IOException;
	private native synchronized void destConnector() throws IOException;
	private native synchronized FileStatus getFileStatus0(Path path) throws IOException;
	private native synchronized List<Path> getDirEntries0(Path path) throws IOException;
	private native synchronized boolean mkdirs0(Path path, short permissions) throws IOException;
	private native synchronized boolean rename0(Path src, Path dst) throws IOException;
	private native synchronized boolean delete0(Path path, boolean recursive) throws IOException;
	private native synchronized void setPermission0(Path path, short permission) throws IOException;
	private native synchronized void setOwner0(Path path, String username, String groupname) throws IOException;
	private native synchronized BlockLocation[] getFileBlockLocations0(FileStatus file, long start, long end) throws IOException;
}
