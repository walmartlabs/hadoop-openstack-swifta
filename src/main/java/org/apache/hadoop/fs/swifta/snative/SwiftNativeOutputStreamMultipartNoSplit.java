/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.swifta.snative;

import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HADOOP_TMP_DIR;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.hadoop.fs.swifta.util.ThreadManager;

/**
 * Output stream, buffers data on local disk. Writes to Swift on the close() method, unless the file
 * is significantly large that it is being written as partitions. In this case, the first partition
 * is written on the first write that puts data over the partition, as may later writes. The close()
 * then causes the final partition to be written, along with a partition manifest.
 */
public class SwiftNativeOutputStreamMultipartNoSplit extends SwiftOutputStream {
  public static final int ATTEMPT_LIMIT = 3;
  private long filePartSize;
  private static final Log LOG = LogFactory.getLog(SwiftNativeOutputStreamMultipartNoSplit.class);
  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private int partNumber;
  private long blockOffset;
  private long bytesWritten;
  private long bytesUploaded;
  private boolean partUpload = false;
  private final byte[] oneByte = new byte[1];

  /**
   * Create an output stream.
   * 
   * @param conf configuration to use
   * @param nativeStore native store to write through
   * @param key the key to write
   * @param partSizeKb the partition size
   * @throws IOException the exception
   */
  public SwiftNativeOutputStreamMultipartNoSplit(Configuration conf,
      SwiftNativeFileSystemStore nativeStore, String key, long partSizeKb) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.nativeStore = nativeStore;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    this.partNumber = 1;
    this.blockOffset = 0;
    this.filePartSize = 1024L * partSizeKb;
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get(HADOOP_TMP_DIR));
    if (!dir.exists()) {
      if (!dir.mkdirs() && !dir.exists()) {
        throw new SwiftException("Cannot create Swift buffer directory: " + dir);
      }
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  /**
   * Flush the local backing stream. This does not trigger a flush of data to the remote blobstore.
   * 
   * @throws IOException the exception
   */
  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  /**
   * Check that the output stream is open.
   *
   * @throws SwiftException if it is not
   */
  private synchronized void verifyOpen() throws SwiftException {
    if (closed) {
      throw new SwiftException("Output stream is closed");
    }
  }

  /**
   * Close the stream. This will trigger the upload of all locally cached data to the remote
   * blobstore.
   * 
   * @throws IOException IO problems uploading the data.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      closed = true;
      // formally declare as closed.
      backupStream.close();
      backupStream = null;
      Path keypath = new Path(key);
      long len = backupFile.length();
      if (len >= this.filePartSize) {
        inParallelPartUpload();
        nativeStore.createManifestForPartUpload(keypath, this.bytesUploaded);
      } else {
        uploadOnClose(keypath);
      }
    } finally {
      // do nothing
    }
    assert backupStream == null : "backup stream has been reopened";
  }

  @SuppressWarnings("rawtypes")
  private void inParallelPartUpload() throws FileNotFoundException {
    long len = backupFile.length();
    long remain = len;
    long off = 0;
    int numberOfParts = (int) (len / this.filePartSize);
    numberOfParts = len % this.filePartSize > 0 ? numberOfParts + 1 : numberOfParts;
    int maxThreads = nativeStore.getMaxInParallelUpload() < 1 ? numberOfParts
        : nativeStore.getMaxInParallelUpload();
    ThreadManager tm = new ThreadManager(maxThreads, maxThreads);
    List<Future> uploads = new ArrayList<Future>();
    while (remain > 0) {
      long uploadLen = remain > this.filePartSize ? filePartSize : remain;
      uploads.add(this.doUpload(tm,
          new RangeInputStream(new FileInputStream(backupFile), off, uploadLen, Boolean.TRUE),
          partNumber++, uploadLen));
      off += uploadLen;
      remain -= uploadLen;
    }
    this.waitToFinish(uploads);

  }

  /**
   * Wait to finish.
   */
  @SuppressWarnings("rawtypes")
  public boolean waitToFinish(List<Future> tasks) {
    for (Future task : tasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        e.printStackTrace();
      } finally {
        task.cancel(Boolean.TRUE);
      }
    }
    tasks.clear();
    tasks = null;
    return Boolean.TRUE;
  }

  @SuppressWarnings("rawtypes")
  Future doUpload(final ThreadManager tm, final InputStream in, final int partNumber,
      final long len) {
    return tm.getPool().submit(new Callable<Boolean>() {
      public Boolean call() throws Exception {
        try {
          // Wait write to finish
          nativeStore.uploadFilePart(new Path(key), partNumber, in, len);
          return Boolean.TRUE;
        } catch (IOException e) {
          LOG.error(e.getMessage());
          return false;
        }
      }
    });
  }

  /**
   * Upload a file when closed, either in one go, or, if the file is already partitioned, by
   * uploading the remaining partition and a manifest.
   * 
   * @param keypath key as a path
   * @throws IOException IO Problems
   */
  private void uploadOnClose(Path keypath) throws IOException {
    boolean uploadSuccess = false;
    int attempt = 0;
    while (!uploadSuccess) {
      try {
        ++attempt;
        bytesUploaded += uploadFileAttempt(keypath, attempt);
        uploadSuccess = true;
      } catch (IOException e) {
        LOG.error("Upload failed! You may not have write permission.");
        if (attempt > ATTEMPT_LIMIT) {
          throw e;
        }
      }
    }
  }

  private long uploadFileAttempt(Path keypath, int attempt) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG, "Closing write of file %s;" + " localfile=%s of length %d - attempt %d",
        key, backupFile, uploadLen, attempt);
    FileInputStream in = null;
    try {
      in = new FileInputStream(backupFile);
      nativeStore.uploadFile(keypath, in, uploadLen);
    } finally {
      IOUtils.closeQuietly(in);
    }
    return uploadLen;
  }

  @Override
  protected void finalize() throws Throwable {
    if (!closed) {
      LOG.warn("stream not closed");
    }
    if (backupFile != null) {
      LOG.warn("Leaking backing file " + backupFile);
    }
  }


  @Override
  public void write(int intByte) throws IOException {
    // insert to a one byte array
    oneByte[0] = (byte) intByte;
    // then delegate to the array writing routine
    write(oneByte, 0, 1);
  }

  @Override
  public synchronized void write(byte[] buffer, int offset, int len) throws IOException {
    // validate args
    if (offset < 0 || len < 0 || (offset + len) > buffer.length) {
      throw new IndexOutOfBoundsException("Invalid offset/length for write");
    }
    // validate the output stream
    verifyOpen();
    // any remaining data is now written
    writeToBackupStream(buffer, offset, len);
  }

  /**
   * Write to the backup stream. Guarantees:
   * <ol>
   * <li>backupStream is open</li>
   * <li>blockOffset + len &lt; filePartSize</li>
   * </ol>
   * 
   * @param buffer buffer to write
   * @param offset offset in buffer
   * @param len length of write.
   * @throws IOException backup stream write failing
   */
  private void writeToBackupStream(byte[] buffer, int offset, int len) throws IOException {
    assert len >= 0 : "remainder to write is negative";
    SwiftUtils.debug(LOG, " writeToBackupStream(offset=%d, len=%d)", offset, len);
    if (len == 0) {
      // no remainder -downgrade to noop
      return;
    }

    // write the new data out to the backup stream
    backupStream.write(buffer, offset, len);
    // increment the counters
    blockOffset += len;
    bytesWritten += len;
  }

  /**
   * Get the file partition size.
   * 
   * @return the partition size
   */
  @Override
  public long getFilePartSize() {
    return filePartSize;
  }

  /**
   * Query the number of partitions written This is intended for testing.
   * 
   * @return the of partitions already written to the remote FS
   */
  @Override
  public synchronized int getPartitionsWritten() {
    return partNumber - 1;
  }

  /**
   * Get the number of bytes written to the output stream. This should always be less than or equal
   * to bytesUploaded.
   * 
   * @return the number of bytes written to this stream
   */
  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Get the number of bytes uploaded to remote Swift cluster. bytesUploaded -bytesWritten = the
   * number of bytes left to upload
   * 
   * @return the number of bytes written to the remote endpoint
   */
  @Override
  public long getBytesUploaded() {
    return bytesUploaded;
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStreamMultipartNoSplit{" + ", key='" + key + '\'' + ", backupFile="
        + backupFile + ", closed=" + closed + ", filePartSize=" + filePartSize + ", partNumber="
        + partNumber + ", blockOffset=" + blockOffset + ", partUpload=" + partUpload
        + ", nativeStore=" + nativeStore + ", bytesWritten=" + bytesWritten + ", bytesUploaded="
        + bytesUploaded + '}';
  }
}
