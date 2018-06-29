/*
 * Copyright (c) [2018]-present, Walmart Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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

  /**
   * The constructor for RangeInputStream.
   * @param in the input stream
   * @param offset the offset
   * @param length the length
   * @param isClose whether closable
   */
  public RangeInputStream(InputStream in, long offset, long length, boolean isClose) {
    this.in = in;
    this.cur = 0;
    this.length = length;
    this.offset = offset;
    this.isClose = isClose;
  }

  @Override
  public int read() throws IOException {
    byte[] bytes = new byte[1];
    int bytesRead = read(bytes, 0, 1);

    if (bytesRead == -1) {
      return bytesRead;
    }
    return bytes[0];
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    while (cur < offset) {
      long skippedBytes = in.skip(offset - cur);
      cur += skippedBytes;
    }

    long bytesRemaining = (length + offset) - cur;
    if (bytesRemaining <= 0) {
      return -1;
    }

    len = (int) Math.min(len, bytesRemaining);
    int bytesRead = in.read(bytes, off, len);
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
