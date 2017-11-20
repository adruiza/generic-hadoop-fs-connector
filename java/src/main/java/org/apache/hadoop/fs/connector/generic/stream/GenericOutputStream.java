package org.apache.hadoop.fs.connector.generic.stream;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;

public class GenericOutputStream extends OutputStream {

	public final static Log LOG = LogFactory.getLog(GenericOutputStream.class);

	private int fd = -1;
	private Path path = null;
	private short permission = 0;
	private boolean overwrite = false;
	private boolean append = false;
	private Statistics statistics = null;

	public GenericOutputStream(Path path, FsPermission permission, boolean overwrite, Statistics statistics) throws IOException {
		super();
		this.path = path;
		this.permission = permission.toShort();
		this.overwrite = overwrite;
		this.statistics = statistics;
		open0(path);
	}

	public GenericOutputStream(Path path, Statistics statistics) throws IOException {
		super();
		this.path = path;
		this.append = true;
		this.statistics = statistics;
		open0(path);
	}

	@Override
	public void write(int b) throws IOException {
		LOG.debug("Write 1B to file " + path);

		write0(b);

		statistics.incrementBytesWritten(1);
		statistics.incrementWriteOps(1);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		LOG.debug("Write " + len + "B to file " + path);

		if(b == null) throw new NullPointerException();
		if(off < 0 || off > b.length || len < 0 || len > b.length - off) throw new IndexOutOfBoundsException();
		writeBytes(b, off, len);

		statistics.incrementBytesWritten(len);
		statistics.incrementWriteOps(1);
	}

	@Override
	public void flush() throws IOException {
		return;
	}

	@Override
	public void close() throws IOException {
		LOG.debug("Close file " + path);

		close0();
	}

	private native synchronized void open0(Path path) throws FileNotFoundException;
	private native synchronized void write0(int b) throws IOException;
	private native synchronized void writeBytes(byte b[], int off, int len) throws IOException;
	private native synchronized void close0() throws IOException;
	private native synchronized void flush0() throws IOException;
}
