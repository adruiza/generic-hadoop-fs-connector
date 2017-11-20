package org.apache.hadoop.fs.connector.generic.stream;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.EOFException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;

public class GenericInputStream extends FSInputStream {

	public final static Log LOG = LogFactory.getLog(GenericInputStream.class);

	private int fd = -1;
	private Path path = null;
	private long fileLength = 0L;
	private long offset = 0L;
	private Statistics statistics = null;

	public GenericInputStream(Path path, long fileLength, Statistics statistics) throws IOException {
		super();
		this.path = path;
		this.fileLength = fileLength;
		this.statistics = statistics;
		open0(path);
	}

	@Override
	public synchronized int read() throws IOException {
		int res;

		LOG.debug("Read 1B from file " + path + " of size " + fileLength + "B on offset=" + offset);

		res = read0();
		if(res == -1) return -1; // EOF
		offset += 1;
		statistics.incrementBytesRead(1);
		statistics.incrementReadOps(1);
		return res;
	}

	@Override
	public synchronized int read(byte b[], int off, int len) throws IOException {
		int res, altLen;

		LOG.debug("Read " + len + "B from file " + path + " of size " + fileLength + "B on offset=" + offset);

		if(b == null) throw new NullPointerException();
		if(len == 0) return 0;
		if(off < 0 || off > b.length || len < 0 || len > b.length - off) throw new IndexOutOfBoundsException();
		if(len > fileLength - offset) altLen = (int) (fileLength - offset);
		else altLen = len;
		res = readBytes(b, off, altLen);
		if(res == 0) return -1; // EOF
		offset += res;
		statistics.incrementBytesRead(res);
		statistics.incrementReadOps(1);
		return res;
	}

	@Override
	public synchronized long getPos() throws IOException {
		return offset;
	}

	@Override
	public synchronized void seek(long pos) throws IOException {
		LOG.debug("Seek on file " + path + " of size " + fileLength + "B to position " + pos);

		if(pos > fileLength) throw new EOFException("Cannot seek after EOF: pos=" + pos + ", fileLength=" + fileLength);
		else {
			seek0(pos);
			offset = pos;
		}
	}

	@Override
	public synchronized boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}

	@Override
	public synchronized void close() throws IOException {
		LOG.debug("Close file " + path);

		close0();
	}

	private native synchronized void open0(Path path) throws FileNotFoundException;
	private native synchronized int read0() throws IOException;
	private native synchronized int readBytes(byte b[], int off, int len) throws IOException;
	private native synchronized void seek0(long pos) throws IOException;
	private native synchronized void close0() throws IOException;
}
