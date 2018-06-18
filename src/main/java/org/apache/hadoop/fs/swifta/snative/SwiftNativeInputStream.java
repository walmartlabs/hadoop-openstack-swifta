/*
 * Copyright (c) [2011]-present, Walmart Inc.
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

import java.io.EOFException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConnectionClosedException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.http.HttpBodyContent;
import org.apache.hadoop.fs.swifta.http.HttpInputStreamWithRelease;
import org.apache.hadoop.fs.swifta.metrics.MetricsFactory;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * The input stream from remote Swift blobs. The class attempts to be buffer aware, and react to a
 * forward seek operation by trying to scan ahead through the current block of data to find it. This
 * accelerates some operations that do a lot of seek()/read() actions, including work (such as in
 * the MR engine) that do a seek() immediately after an open().
 */
class SwiftNativeInputStream extends FSInputStream {

  private static final Log LOG = LogFactory.getLog(SwiftNativeInputStream.class);

  private static final MetricsFactory metric =
      MetricsFactory.getMetricsFactory(SwiftNativeInputStream.class);

  /**
   * File nativeStore instance.
   */
  private final SwiftNativeFileSystemStore nativeStore;


  private final boolean isLazy;

  /**
   * File path.
   */
  private final Path path;

  /**
   * Hadoop statistics. Used to get info about number of reads, writes, etc.
   */
  private final FileSystem.Statistics statistics;

  /**
   * Data input stream.
   */
  private HttpInputStreamWithRelease httpStream;

  /**
   * Current position.
   */
  private long pos = 0;

  /**
   * Length of the file picked up at start time.
   */
  private final long contentLength;

  /**
   * Why the stream is closed.
   */
  private String reasonClosed = "unopened";

  /**
   * Offset in the range requested last.
   */
  // private long rangeOffset = 0;

  private long nextReadPosition = 0;

  public SwiftNativeInputStream(SwiftNativeFileSystemStore storeNative,
      FileSystem.Statistics statistics, Path path) throws IOException {
    this.nativeStore = storeNative;
    this.statistics = statistics;
    this.path = path;
    // initial buffer fill
    HttpBodyContent content = storeNative.getObject(path);
    this.httpStream = content.getInputStream();
    this.contentLength = content.getContentLength();
    this.isLazy = nativeStore.isLazyseek();
    metric.increase(path.toString(), this);
    metric.report();
  }

  /**
   * Move to a new position within the file relative to where the pointer is now. Always call from a
   * synchronized clause.
   * 
   * @param offset offset
   */
  private synchronized void incPos(int offset) {
    pos += offset;
    nextReadPosition = pos;
    SwiftUtils.debug(LOG, "Inc: pos=%d nextReadPosition=%d", pos, nextReadPosition);
  }

  /**
   * Update the start of the buffer; always call from a sync'd clause.
   * 
   * @param seekPos position sought.
   * @param contentLength content length provided by response (may be -1)
   */
  private synchronized void updateStartOfBufferPosition(long seekPos) {
    // reset the seek pointer
    pos = seekPos;
    nextReadPosition = seekPos;
    SwiftUtils.trace(LOG, "Move: pos=%d; nextReadPosition=%d; contentLength=%d", pos,
        nextReadPosition, contentLength);
  }

  @Override
  public synchronized int read() throws IOException {
    verifyOpen();
    int result = -1;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read() api be invoked!");
    }
    try {
      result = httpStream.read();
    } catch (IOException e) {
      String msg = "IOException while reading " + path + ": ' +e, attempting to reopen.";
      LOG.debug(msg, e);
      if (reopenBuffer()) {
        result = httpStream.read();
      }
    }
    if (result != -1) {
      incPos(1);
    }
    if (statistics != null && result != -1) {
      statistics.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte[] bytes, int off, int len) throws IOException {
    SwiftUtils.debug(LOG, "read(buffer, %d, %d)", off, len);
    SwiftUtils.validateReadArgs(bytes, off, len);
    int result = -1;
    if (this.contentLength == 0 || nextReadPosition >= contentLength) {
      return result;
    }
    try {
      verifyOpen();
      if (isLazy) {
        seekStream(nextReadPosition);
      }
      result = httpStream.read(bytes, off, len);
    } catch (IOException e) {
      // other IO problems are viewed as transient and re-attempted
      LOG.info("Received IOException while reading '" + path + "', attempting to reopen: " + e);
      LOG.debug("IOE on read()" + e, e);
      if (reopenBuffer()) {
        result = httpStream.read(bytes, off, len);
      }
    }
    if (result > 0) {
      incPos(result);
      if (statistics != null) {
        statistics.incrementBytesRead(result);
      }
    }

    return result;
  }

  /**
   * Re-open the buffer.
   * 
   * @return true iff more data could be added to the buffer
   * @throws IOException if not
   */
  private synchronized boolean reopenBuffer() throws IOException {
    innerClose("reopening buffer to trigger refresh");
    boolean success = false;
    try {
      fillBuffer(pos);
      success = true;
    } catch (EOFException eof) {
      // the EOF has been reached
      this.reasonClosed = "End of file";
    }
    return success;
  }

  /**
   * close the stream. After this the stream is not usable -unless and until it is re-opened (which
   * can happen on some of the buffer ops) This method is thread-safe and idempotent.
   *
   * @throws IOException on IO problems.
   */
  @Override
  public void close() throws IOException {
    innerClose("closed");
  }

  private synchronized void innerClose(String reason) throws IOException {
    try {
      if (httpStream != null) {
        reasonClosed = reason;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing HTTP input stream : " + reason);
        }
        IOUtils.closeStream(httpStream);
      }
    } finally {
      httpStream = null;
      metric.remove(this);
      metric.report();
    }
  }

  /**
   * Assume that the connection is not closed: throws an exception if it is.
   * 
   * @throws SwiftConnectionClosedException the exception
   */
  private void verifyOpen() throws SwiftConnectionClosedException {
    if (httpStream == null) {
      throw new SwiftConnectionClosedException(reasonClosed);
    }
  }

  @Override
  public String toString() {
    return "SwiftNativeInputStream" + " position=" + pos + " "
        + (httpStream != null ? httpStream.toString() : (" no input stream: " + reasonClosed));
  }

  /**
   * Read through the specified number of bytes. The implementation iterates a byte a time, which
   * may seem inefficient compared to the read(bytes[]) method offered by input streams. However, if
   * you look at the code that implements that method, it comes down to read() one char at a time
   * -only here the return value is discarded.
   *
   * <p/>
   * This is a no-op if the stream is closed.
   * 
   * @param bytes number of bytes to read.
   * @throws IOException IO problems
   * @throws SwiftException if a read returned -1.
   */
  private long chompBytes(long bytes) throws IOException {
    int count = 0;
    if (httpStream != null) {
      count = (int) httpStream.skip(bytes);
      incPos(count);
    }
    return count;
  }

  /**
   * Seek to an offset. If the data is already in the buffer, move to it.
   * 
   * @param targetPos target position
   * @throws IOException on any problem
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos < 0) {
      throw new IOException("Negative Seek offset not supported.");
    }

    nextReadPosition = targetPos;
    if (!isLazy) {
      seekStream(targetPos);
    }
  }

  public synchronized void realSeek(long targetPos) throws IOException {
    if (targetPos < 0) {
      throw new IOException("Negative Seek offset not supported");
    }
    // there's some special handling of near-local data
    // as the seek can be omitted if it is in/adjacent
    long offset = targetPos - pos;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Seek to " + targetPos + "; current pos =" + pos + "; offset=" + offset);
    }
    if (offset == 0) {
      LOG.debug("seek is no-op");
      return;
    }

    if (offset < 0) {
      LOG.debug("seek is backwards");
    } else if ((targetPos <= this.contentLength)) {
      // if the seek is in range of that requested, scan forwards
      // instead of closing and re-opening a new HTTP connection
      if (LOG.isDebugEnabled()) {
        SwiftUtils.debug(LOG, "seek is within current stream" + "; pos= %d ; targetPos=%d; "
            + "offset= %d ; nextReadPosition=%d", pos, targetPos, offset, nextReadPosition);
      }

      try {
        LOG.debug("chomping ");
        chompBytes(offset);
      } catch (IOException e) {
        // this is assumed to be recoverable with a seek -or more likely to fail
        LOG.debug("while chomping ", e);
      }
      if (targetPos - pos == 0) {
        LOG.trace("chomping successful");
        return;
      }
      LOG.trace("chomping failed");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Seek is beyond file length of " + this.contentLength);
      }
    }

    innerClose("seeking to " + targetPos);
    fillBuffer(targetPos);
  }

  /**
   * Lazy seek.
   * 
   * @throws IOException the exception.
   */
  private synchronized void seekStream(long targetPos) throws IOException {
    if (targetPos == pos) {
      return;
    }
    realSeek(targetPos);
  }

  /**
   * Fill the buffer from the target position If the target position == current position, the read
   * still goes ahead; this is a way of handling partial read failures.
   * 
   * @param targetPos target position
   * @throws IOException IO problems on the read
   */
  private synchronized void fillBuffer(long targetPos) throws IOException {
    SwiftUtils.debug(LOG, "Fetching %d bytes starting at %d", (this.contentLength - targetPos + 1),
        targetPos);
    HttpBodyContent blob = nativeStore.getObject(path, targetPos, this.contentLength);
    httpStream = blob.getInputStream();
    updateStartOfBufferPosition(targetPos);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return nextReadPosition;
  }

  /**
   * This FS doesn't explicitly support multiple data sources, so return false here.
   * 
   * @param targetPos the desired target position
   * @return true if a new source of the data has been set up as the source of future reads
   * @throws IOException IO problems
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
