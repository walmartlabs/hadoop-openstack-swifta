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


package org.apache.hadoop.fs.swifta.http;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConnectionClosedException;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * This replaces the input stream release class from JetS3t and AWS; # Failures in the constructor
 * are relayed up instead of simply logged. # it is set up to be more robust at teardown # release
 * logic is thread safe Note that the thread safety of the inner stream contains no thread safety
 * guarantees -this stream is not to be read across streams. The thread safety logic here is to
 * ensure that even if somebody ignores that rule, the release code does not get entered twice -and
 * that any release in one thread is picked up by read operations in all others.
 */
public class HttpInputStreamWithRelease extends InputStream {

  private static final Log LOG = LogFactory.getLog(HttpInputStreamWithRelease.class);
  private final URI uri;
  private final HttpMethod method;

  /**
   * Glag to say the stream is released -volatile so that read operations pick it up even while
   * unsynchronized.
   */
  private volatile boolean released;

  /**
   * Volatile flag to verify that data is consumed.
   */
  private volatile boolean dataConsumed;
  private InputStream oldInStream;

  /**
   * Optimize performance.
   */
  private BufferedInputStream inStream;

  /**
   * In debug builds, this is filled in with the construction-time stack, which is then included in
   * logs from the finalize(), method.
   */
  private final Exception constructionStack;

  /**
   * Why the stream is closed.
   */
  private String reasonClosed = "unopened";

  /**
   * The constructor for HttpInputStreamWithRelease.
   *
   * @param uri the uri
   * @param method the method
   * @param bufferSize the buffer size
   * @throws IOException the exception
   */
  public HttpInputStreamWithRelease(final URI uri, final HttpMethod method, final int bufferSize)
      throws IOException {
    this.uri = uri;
    this.method = method;
    constructionStack = LOG.isDebugEnabled() ? new Exception("stack") : null;
    if (method == null) {
      throw new IllegalArgumentException("Null 'method' parameter ");
    }
    try {
      oldInStream = method.getResponseBodyAsStream();
      inStream = new BufferedInputStream(oldInStream, bufferSize);
    } catch (IOException e) {
      oldInStream = new ByteArrayInputStream(new byte[] {});
      inStream = new BufferedInputStream(oldInStream, bufferSize);
      throw releaseAndRethrow("getResponseBodyAsStream() in constructor -" + e, e);
    }
  }

  @Override
  public void close() throws IOException {
    release("close()", null);
  }

  /**
   * Release logic.
   * 
   * @param reason reason for release (used in debug messages)
   * @param ex exception that is a cause -null for non-exceptional releases
   * @return true if the release took place here
   * @throws IOException if the abort or close operations failed.
   */
  private synchronized boolean release(String reason, Exception ex) throws IOException {
    if (!released) {
      reasonClosed = reason;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Releasing connection to " + uri + ":  " + reason, ex);
        }
        if (method != null) {
          if (!dataConsumed) {
            method.abort();
          }
          method.releaseConnection();
        }

        return true;
      } finally {
        if (inStream != null) {
          IOUtils.closeStream(inStream);
          inStream = null;
        }
        if (oldInStream != null) {
          IOUtils.closeStream(oldInStream);
          oldInStream = null;
        }
        // if something went wrong here, we do not want the release() operation
        // to try and do anything in advance.
        released = true;
        dataConsumed = true;
      }
    }
    return false;
  }

  /**
   * Release the method, using the exception as a cause.
   * 
   * @param operation operation that failed
   * @param ex the exception which triggered it.
   * @return the exception to throw
   */
  private IOException releaseAndRethrow(String operation, IOException ex) {
    try {
      release(operation, ex);
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception during release: " + operation + " - " + ioe, ioe);
      }
      // make this the exception if there was none before
      if (ex == null) {
        ex = ioe;
      }
    }
    return ex;
  }

  /**
   * Assume that the connection is not released: throws an exception if there is.
   * 
   * @throws SwiftConnectionClosedException the exception
   */
  private synchronized void assumeNotReleased() throws SwiftConnectionClosedException {
    if (released || inStream == null) {
      throw new SwiftConnectionClosedException(reasonClosed);
    }
  }

  @Override
  public synchronized int available() throws IOException {
    assumeNotReleased();
    try {
      return inStream.available();
    } catch (IOException e) {
      throw releaseAndRethrow("available() failed -" + e, e);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    assumeNotReleased();
    int read = 0;
    try {
      read = inStream.read();
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("EOF exception " + e, e);
      }
      read = -1;
    } catch (IOException e) {
      throw releaseAndRethrow("read()", e);
    }
    if (read < 0) {
      dataConsumed = true;
      release("read() -all data consumed", null);
    }
    return read;
  }

  @Override
  public synchronized int read(byte[] bytes, int off, int len) throws IOException {

    SwiftUtils.validateReadArgs(bytes, off, len);
    // if the stream is already closed, then report an exception.
    assumeNotReleased();
    // now read in a buffer, reacting differently to different operations
    int read;
    try {
      read = inStream.read(bytes, off, len);
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("EOF exception " + e, e);
      }
      read = -1;
    } catch (IOException e) {
      throw releaseAndRethrow("read(b, off, " + len + ")", e);
    }
    if (read < 0) {
      dataConsumed = true;
      release("read() -all data consumed", null);
    }
    return read;
  }

  /**
   * Finalize does release the stream, but also logs at WARN level including the URI at fault.
   */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (!released) {
        if (release("finalize()", constructionStack)) {
          LOG.warn("input stream of " + uri + " not closed properly cleaned up in finalize()");
        }
      }
    } catch (Exception e) {
      // swallow anything that failed here
      LOG.warn("Exception while releasing " + uri + " in finalizer", e);
    }
    super.finalize();
  }

  @Override
  public String toString() {
    return "HttpInputStreamWithRelease working with " + Objects.toString(uri) + " released = "
        + Objects.toString(released) + " dataConsumed = " + Objects.toString(dataConsumed);
  }
}
