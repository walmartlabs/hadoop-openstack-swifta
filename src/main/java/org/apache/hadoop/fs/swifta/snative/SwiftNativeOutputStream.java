/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.fs.swifta.snative;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftInternalStateException;
import org.apache.hadoop.fs.swifta.metrics.MetricsFactory;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.hadoop.fs.swifta.util.ThreadManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Output stream, buffers data on local disk. Writes to Swift on the close() method, unless the file is significantly large that it is being written as partitions. In this case, the first partition is
 * written on the first write that puts data over the partition, as may later writes. The close() then causes the final partition to be written, along with a partition manifest.
 */
class SwiftNativeOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(SwiftNativeOutputStream.class);
  private static final MetricsFactory metric = MetricsFactory.getMetricsFactory(SwiftNativeOutputStream.class);

  private static final int ATTEMPT_LIMIT = 3;
  private long filePartSize;
  private String key;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private long blockOffset;
  private long bytesWritten;
  private long bytesUploaded;
  private boolean partUpload = false;
  final byte[] oneByte = new byte[1];
  final String backupDir;
  private AtomicInteger num;
  @SuppressWarnings("rawtypes")
  Map<AsynchronousFileChannel, Future> results;
  List<File> backupFiles;
  ThreadManager tm;

  private AtomicInteger partNumber;

  final File dir;

  /**
   * Create an output stream.
   * 
   * @param conf configuration to use
   * @param nativeStore native store to write through
   * @param key the key to write
   * @param partSizeKB the partition size
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public SwiftNativeOutputStream(Configuration conf, SwiftNativeFileSystemStore nativeStore, String key, long partSizeKB) throws IOException {
    dir = new File(conf.get("hadoop.tmp.dir"));
    this.key = key;
    this.nativeStore = nativeStore;
    this.blockOffset = 0;
    this.partNumber = new AtomicInteger(1);
    num = new AtomicInteger(1);
    this.filePartSize = 1024L * partSizeKB;
    backupDir = UUID.randomUUID().toString();
    results = new HashMap<AsynchronousFileChannel, Future>();
    backupFiles = new ArrayList<File>();
    metric.increase(key, this);
    metric.report();
  }

  private AsynchronousFileChannel openForWrite() throws IOException {
    File tmp = newBackupFile();
    backupFiles.add(tmp);
    return AsynchronousFileChannel.open(Paths.get(tmp.getAbsolutePath()), StandardOpenOption.WRITE);
  }

  private File newBackupFile() throws IOException {
    File newDir = new File(dir, backupDir);
    if (!newDir.mkdirs() && !newDir.exists()) {
      throw new SwiftException("Cannot create Swift buffer directory: " + dir);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("num:" + num + "; dir:" + dir + ":backupDir:" + backupDir);
    }
    File result = File.createTempFile("output-" + num.getAndIncrement(), ".tmp", newDir);
    result.deleteOnExit();
    return result;
  }

  /**
   * Flush the local backing stream. This does not trigger a flush of data to the remote blobstore.
   * 
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    // Not implement.
    // backupStream.force(Boolean.FALSE);
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
   * Close the stream. This will trigger the upload of all locally cached data to the remote blobstore.
   * 
   * @throws IOException IO problems uploading the data.
   */
  @SuppressWarnings("rawtypes")
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      this.waitToFinish(results);
      closed = true;
      Path keypath = new Path(key);
      if (partUpload) {
        List<Future> uploads = uploadParts();
        // Prevent incomplete read before a full upload.
        this.waitToFinish(uploads);
        nativeStore.createManifestForPartUpload(keypath);
      } else {
        uploadOnClose(keypath);
      }

    } finally {
      cleanBackupFiles();
      metric.remove(this);
      metric.report();
    }
    // assert backupStream == null : "backup stream has been reopened";
  }

  @SuppressWarnings("rawtypes")
  private List<Future> uploadParts() {
    List<Future> uploads = new ArrayList<Future>();
    int len = backupFiles.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using muli-parts upload with threads " + len);
    }
    for (final File f : backupFiles) {
      uploads.add(this.getThreads(len).submit(new Callable<Boolean>() {
        public Boolean call() throws Exception {
          try {
            partUpload(Boolean.TRUE, f);
            return true;
          } catch (IOException e) {
            return false;
          }
        }
      }));
    }
    tm.shutdown();
    return uploads;
  }

  private void cleanBackupFiles() {
    for (File f : backupFiles) {
      delete(f);
      f = null;
    }
    backupFiles.clear();
    backupFiles = null;
  }

  /**
   * Upload a file when closed, either in one go, or, if the file is already partitioned, by uploading the remaining partition and a manifest.
   * 
   * @param keypath key as a path
   * @throws IOException IO Problems
   */
  private void uploadOnClose(Path keypath) throws IOException {
    if (backupFiles.size() < 1) {
      AsynchronousFileChannel channel = openForWrite();
      channel.close();
    }
    if (backupFiles.size() > 1) {
      throw new SwiftException("Too many backup file to upload. size = " + backupFiles.size());
    }
    boolean uploadSuccess = false;
    int attempt = 0;
    File backupFile = backupFiles.get(0);
    while (!uploadSuccess) {
      try {
        ++attempt;
        bytesUploaded += uploadFileAttempt(keypath, attempt, backupFile);
        uploadSuccess = true;
      } catch (IOException e) {
        LOG.info("Upload failed " + e, e);
        if (attempt > ATTEMPT_LIMIT) {
          throw e;
        }
      }
    }
  }

  private long uploadFileAttempt(Path keypath, int attempt, File backupFile) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG, "Closing write of file %s;" + " localfile=%s of length %d - attempt %d", key, backupFile, uploadLen, attempt);
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(backupFile);
      nativeStore.uploadFile(keypath, inputStream, uploadLen);
    } catch (IOException e) {
      throw e;
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return uploadLen;
  }

  private void delete(File file) {
    if (file != null) {
      SwiftUtils.debug(LOG, "deleting %s", file);
      if (!file.delete()) {
        LOG.warn("Could not delete " + file);
        file.deleteOnExit();
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    // insert to a one byte array
    oneByte[0] = (byte) b;
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
    if (LOG.isDebugEnabled()) {
      SwiftUtils.debug(LOG, " write(offset=%d, len=%d)", offset, len);
    }
    // Map<Integer, Future<Boolean>> uploads = new HashMap<Integer, Future<Boolean>>();
    // if the size of file is greater than the partition limit
    // int i = 0;
    // int max = (int) ((len - offset) / filePartSize + 1);
    while (blockOffset + len >= filePartSize) {
      // - then partition the blob and upload as many partitions
      // are needed.
      // how many bytes to write for this partition.
      int subWriteLen = (int) (filePartSize - blockOffset);
      if (subWriteLen < 0 || subWriteLen > len) {
        throw new SwiftInternalStateException("Invalid subwrite len: " + subWriteLen + " -buffer len: " + len);
      }
      writeToBackupStream(buffer, offset, subWriteLen);
      // move the offset along and length down
      offset += subWriteLen;
      len -= subWriteLen;
      blockOffset = 0;
      partUpload = true;
      // partUpload(false);
    }
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
    AsynchronousFileChannel backupFile = openForWrite();
    results.put(backupFile, backupFile.write(ByteBuffer.wrap(buffer, offset, len), blockOffset));
    // increment the counters
    blockOffset += len;
    bytesWritten += len;
  }

  private ExecutorService getThreads(int max) {
    if (tm == null) {
      tm = new ThreadManager();
      tm.createThreadManager(max);
    }
    return this.tm.getPool();
  }

  @SuppressWarnings("rawtypes")
  private boolean waitToFinish(List<Future> results) {
    for (Future task : results) {
      try {
        task.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    this.closePool();
    results.clear();
    results = null;
    return Boolean.TRUE;
  }

  private void closePool() {
    tm.cleanup();
    tm = null;
  }

  @SuppressWarnings("rawtypes")
  private boolean waitToFinish(Map<AsynchronousFileChannel, Future> results) {
    Set<Map.Entry<AsynchronousFileChannel, Future>> entrySet = results.entrySet();
    for (Map.Entry<AsynchronousFileChannel, Future> entry : entrySet) {
      AsynchronousFileChannel channel = entry.getKey();
      Future task = entry.getValue();
      try {
        task.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      } finally {
        try {
          if (channel != null) {
            channel.close();
          }
        } catch (IOException e) {
          // Ignore
        }
      }

    }
    this.closePool();
    results.clear();
    results = null;
    return Boolean.TRUE;
  }

  /**
   * Upload a single partition. This deletes the local backing-file, and re-opens it to create a new one.
   * 
   * @param closingUpload is this the final upload of an upload
   * @throws IOException on IO problems
   */
  private void partUpload(boolean closingUpload, File backupFile) throws IOException {

    if (closingUpload && partUpload && backupFile.length() == 0) {
      // skipping the upload if
      // - it is close time
      // - the final partition is 0 bytes long
      // - one part has already been written
      SwiftUtils.debug(LOG, "skipping upload of 0 byte final partition");
      delete(backupFile);
    } else {
      partUpload = true;
      boolean uploadSuccess = false;
      int attempt = 0;
      while (!uploadSuccess) {
        try {
          ++attempt;
          bytesUploaded += uploadFilePartAttempt(attempt, backupFile);
          uploadSuccess = true;
        } catch (IOException e) {
          LOG.info("Upload failed " + e, e);
          if (attempt > ATTEMPT_LIMIT) {
            throw e;
          }
        }
      }
      // delete(backupFile);
      // blockOffset = 0;
      // if (!closingUpload) {
      // // if not the final upload, create a new output stream
      // backupFile = newBackupFile();
      // backupStream = AsynchronousFileChannel.open(Paths.get(backupFile.getAbsolutePath()), StandardOpenOption.WRITE);
      // }
    }
  }

  private long uploadFilePartAttempt(int attempt, File backupFile) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG, "Uploading part %d of file %s;" + " localfile=%s of length %d  - attempt %d", partNumber, key, backupFile, uploadLen, attempt);
    FileInputStream inputStream = null;
    try {
      // this.isDone(results);
      inputStream = new FileInputStream(backupFile);
      nativeStore.uploadFilePart(new Path(key), partNumber.getAndIncrement(), inputStream, uploadLen);
    } catch (IOException e) {
      throw e;
    } finally {
      IOUtils.closeQuietly(inputStream);
    }

    return uploadLen;
  }

  /**
   * Get the file partition size
   * 
   * @return the partition size
   */
  long getFilePartSize() {
    return filePartSize;
  }

  /**
   * Query the number of partitions written This is intended for testing.
   * 
   * @return the of partitions already written to the remote FS
   */
  synchronized int getPartitionsWritten() {
    return partNumber.get() - 1;
  }

  /**
   * Get the number of bytes written to the output stream. This should always be less than or equal to bytesUploaded.
   * 
   * @return the number of bytes written to this stream
   */
  long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Get the number of bytes uploaded to remote Swift cluster. bytesUploaded -bytesWritten = the number of bytes left to upload.
   * 
   * @return the number of bytes written to the remote endpoint
   */
  long getBytesUploaded() {
    return bytesUploaded;
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStream{" + ", key='" + key + '\'' + ", closed=" + closed + ", filePartSize=" + filePartSize + ", blockOffset=" + blockOffset + ", partUpload=" + partUpload
        + ", nativeStore=" + nativeStore + ", bytesWritten=" + bytesWritten + ", bytesUploaded=" + bytesUploaded + '}';
  }
}
