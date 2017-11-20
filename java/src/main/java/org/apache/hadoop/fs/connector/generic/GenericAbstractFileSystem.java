package org.apache.hadoop.fs.connector.generic;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.EnumSet;

import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;

public class GenericAbstractFileSystem extends AbstractFileSystem {

	private final FileSystem fsImpl;

	public GenericAbstractFileSystem(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
		this(uri, new GenericFileSystem(), conf, uri.getScheme(), false);
	}

	public GenericAbstractFileSystem(URI uri, FileSystem fsImpl, Configuration conf, String supportedScheme, boolean authorityNeeded) throws IOException, URISyntaxException {
		super(uri, supportedScheme, authorityNeeded, FileSystem.getDefaultUri(conf).getPort());
		this.fsImpl = fsImpl;
		this.fsImpl.initialize(uri, conf);
	}

	@Override
	public int getUriDefaultPort() {
		return -1;
	}

	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
    FsServerDefaults defaults = fsImpl.getServerDefaults();
		return defaults;
	}

	@Override
	public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
		checkPath(f);
		if (!createParent) {
			final FileStatus stat = getFileStatus(f.getParent());
			if (stat == null) {
				throw new FileNotFoundException("Missing parent:" + f);
			}
			if (!stat.isDirectory()) {
				throw new ParentNotDirectoryException("Parent is not a dir:" + f);
			}
		}
		FSDataOutputStream stream = fsImpl.create(f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
		return stream;
	}

	@Override
	public void mkdir(Path dir, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(dir);
		boolean status = fsImpl.mkdirs(dir, permission);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		boolean status = fsImpl.delete(f, recursive);
		return status;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		FSDataInputStream stream = fsImpl.open(f, bufferSize);
		return stream;
	}

	@Override
	public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		boolean set = fsImpl.setReplication(f, replication);
		return set;
	}

	@Override
	public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
		checkPath(src);
		checkPath(dst);
		fsImpl.rename(src, dst);
	}

	@Override
	public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		fsImpl.setPermission(f, permission);
	}

	@Override
	public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		fsImpl.setOwner(f, username, groupname);
	}

	@Override
	public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		fsImpl.setTimes(f, mtime, atime);
	}

	@Override
	public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		return fsImpl.getFileChecksum(f);
	}

	@Override
	public FileStatus getFileStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		return fsImpl.getFileStatus(f);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		return fsImpl.getFileBlockLocations(f, start, len);
	}

	@Override
	public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
		return fsImpl.getStatus();
	}

	@Override
	public FileStatus[] listStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		checkPath(f);
		return fsImpl.listStatus(f);
	}

	@Override
	public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {
		fsImpl.setVerifyChecksum(verifyChecksum);
	}
}
