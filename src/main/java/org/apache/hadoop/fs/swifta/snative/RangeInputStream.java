package org.apache.hadoop.fs.swifta.snative;

import java.io.IOException;
import java.io.InputStream;

public final class RangeInputStream extends InputStream {
  private long cur;
  private final long offset;
  private final long length;
  private final boolean isClose;
  private long mark = 0;
  private final InputStream in;

  public RangeInputStream(InputStream in, long offset, long length, boolean isClose) {
    this.in = in;
    this.cur = 0;
    this.length = length;
    this.offset = offset;
    this.isClose = isClose;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int bytesRead = read(b, 0, 1);

    if (bytesRead == -1) {
      return bytesRead;
    }
    return b[0];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    while (cur < offset) {
      long skippedBytes = in.skip(offset - cur);
      cur += skippedBytes;
    }

    long bytesRemaining = (length + offset) - cur;
    if (bytesRemaining <= 0)
      return -1;

    len = (int) Math.min(len, bytesRemaining);
    int bytesRead = in.read(b, off, len);
    cur += bytesRead;
    return bytesRead;
  }

  @Override
  public synchronized void mark(int readlimit) {
    mark = cur;
    in.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    cur = mark;
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isClose) {
      in.close();
    }
  }

  @Override
  public int available() throws IOException {
    long bytesRemaining;
    if (cur < offset) {
      bytesRemaining = length;
    } else {
      bytesRemaining = (length + offset) - cur;
    }

    return (int) Math.min(bytesRemaining, in.available());
  }
}
