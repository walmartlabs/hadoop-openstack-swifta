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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Output stream, buffers data on local disk. Writes to Swift on the close() method, unless the file is significantly large that it is being written as partitions. In this case, the first partition is
 * written on the first write that puts data over the partition, as may later writes. The close() then causes the final partition to be written, along with a partition manifest.
 */
class SwiftNativeOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(SwiftNativeOutputStream.class);
  private static final MetricsFactory metric = MetricsFactory.getMetricsFactory(SwiftNativeOutputStream.class);

  private static final int ATTEMPT_LIMIT = 3;
  private static final int FILE_MAX_THREADS = 10;
  private long filePartSize;
  private String key;
  private AsynchronousFileChannel backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private long blockOffset;
  private long bytesWritten;
  private AtomicLong bytesUploaded;
  private volatile boolean partUpload = false;
  final byte[] oneByte = new byte[1];
  final String backupDir;
  @SuppressWarnings("rawtypes")
  private Map<AsynchronousFileChannel, Future> results;
  // Part number and temporary files.
  private Map<Integer, File> backupFiles;
  private final ThreadManager executor;

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
    bytesUploaded = new AtomicLong(0);
    this.filePartSize = 1024L * partSizeKB;
    backupDir = UUID.randomUUID().toString();
    results = new HashMap<AsynchronousFileChannel, Future>();
    backupFiles = new HashMap<Integer, File>();
    executor = new ThreadManager(FILE_MAX_THREADS);
    backupStream = openForWrite(partNumber.getAndIncrement());
    metric.increase(key, this);
    metric.report();
  }

  private synchronized AsynchronousFileChannel openForWrite(int partNumber) throws IOException {
    File tmp = newBackupFile(partNumber);
    backupFiles.put(partNumber, tmp);
    return AsynchronousFileChannel.open(Paths.get(tmp.getAbsolutePath()), EnumSet.of(StandardOpenOption.WRITE), executor.getPool());
  }

  private File newBackupFile(int partNumber) throws IOException {
    File newDir = new File(dir, backupDir);
    if (!newDir.mkdirs() && !newDir.exists()) {
      throw new SwiftException("Cannot create Swift buffer directory: " + dir);
    }
    String file = SwiftUtils.partitionFilenameFromNumber(partNumber);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Temporary file:" + newDir + "/" + file);
    }
    File result = File.createTempFile(file, ".tmp", newDir);
    result.deleteOnExit();
    return result;
  }

  /**
   * Flush the local backing stream. This does not trigger a flush of data to the remote blobstore.
   * 
   * @throws IOException
   */
  @Override
  public synchronized void flush() throws IOException {
    backupStream.force(Boolean.FALSE);
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
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      this.waitToFinish(results);
      // formally declare as closed.
      if (backupStream != null) {
        backupStream.close();
        backupStream = null;
      }
      closed = true;

      Path keypath = new Path(key);
      if (partUpload) {
        uploadParts();
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
  private void uploadParts() throws IOException {
    final List<Future> uploads = new ArrayList<Future>();
    final int len = backupFiles.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using muli-parts upload with threads " + len);
    }
    final ThreadManager tm = new ThreadManager();
    tm.createThreadManager(len);

    Set<Map.Entry<Integer, File>> entrySet = backupFiles.entrySet();
    for (final Map.Entry<Integer, File> entry : entrySet) {
      uploads.add(tm.getPool().submit(new Callable<Boolean>() {
        public Boolean call() throws Exception {
          try {
            final File f = entry.getValue();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Upload file " + f.getName() + ";partNumber=" + entry.getKey() + ";len=" + f.length());
            }
            partUpload(f, entry.getKey().intValue());
            return Boolean.TRUE;
          } catch (IOException e) {
            return false;
          }
        }
      }));
    }
    tm.shutdown();

    // Prevent incomplete read before a full upload.
    this.waitToFinish(uploads);
    this.cleanUp(tm);
  }

  private void cleanBackupFiles() {
    if (backupFiles != null) {
      Set<Map.Entry<Integer, File>> entrySet = backupFiles.entrySet();
      for (final Map.Entry<Integer, File> entry : entrySet) {
        delete(entry.getValue());
      }
      backupFiles.clear();
    }
  }

  /**
   * Upload a file when closed, either in one go, or, if the file is already partitioned, by uploading the remaining partition and a manifest.
   * 
   * @param keypath key as a path
   * @throws IOException IO Problems
   */
  private void uploadOnClose(Path keypath) throws IOException {
     if (backupFiles.size() < 1) {
       throw new SwiftException("No file to upload!");
     }
    if (backupFiles.size() > 1) {
      throw new SwiftException("Too many backup file to upload. size = " + backupFiles.size());
    }
    boolean uploadSuccess = false;
    int attempt = 0;
    File backupFile = backupFiles.values().iterator().next();
    while (!uploadSuccess) {
      try {
        ++attempt;
        bytesUploaded.addAndGet(uploadFileAttempt(keypath, attempt, backupFile));
        uploadSuccess = true;
      } catch (IOException e) {
        LOG.error("Upload failed " + e, e);
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
    // LOG.info("writeToBackupStream(offset='" + offset + "', len='" + len + "')");
    // validate the output stream
    verifyOpen();
    this.autoWriteToSplittedBackupStream(buffer, offset, len);

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
  private void autoWriteToSplittedBackupStream(byte[] buffer, int offset, int len) throws IOException {
    if (len <= 0) {
      // no remainder -downgrade to noop
      return;
    }
    // SwiftUtils.debug(LOG, " writeToBackupStream(offset=%d, len=%d)", offset, len);
    // LOG.info("offset:" + offset + ";len:" + len);
    // write the new data out to the backup stream
    while (len > 0) {
      // LOG.info("len" + len + ";(blockOffset + len):" + (blockOffset + len) + ";filePartSize:" + filePartSize);
      if ((blockOffset + len) >= filePartSize) {
        int subLen = (int) (filePartSize - blockOffset);
        results.put(backupStream, backupStream.write(ByteBuffer.wrap(buffer, offset, subLen), blockOffset));
        // Don't have to close backupStream here.
        offset += subLen;
        len -= subLen;
        bytesWritten += subLen;
        blockOffset = 0;
        partUpload = true;
        backupStream = openForWrite(partNumber.getAndIncrement());

      } else {
        // LOG.info((i++) + ":last writing " + len + ";blockOffset:" + blockOffset);
        results.put(backupStream, backupStream.write(ByteBuffer.wrap(buffer, offset, len), blockOffset));
        blockOffset += len;
        bytesWritten += len;
        len = 0;
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private boolean waitToFinish(List<Future> tasks) {
    for (Future task : tasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        e.printStackTrace();
      } finally {
        task.cancel(Boolean.FALSE);
      }
    }
    tasks.clear();
    tasks = null;
    return Boolean.TRUE;
  }

  private void cleanUp(ThreadManager tm) {
    if (tm != null) {
      tm.cleanup();
      tm = null;
    }
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
        task.cancel(Boolean.TRUE);
      }

    }
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
  private void partUpload(final File backupFile, final int partNumber) throws IOException {



    if (partUpload && backupFile.length() == 0) {
      // skipping the upload if
      // - it is close time
      // - the final partition is 0 bytes long
      // - one part has already been written
      SwiftUtils.debug(LOG, "skipping upload of 0 byte final partition");
      delete(backupFile);
    } else {
      // partUpload = true;
      boolean uploadSuccess = false;
      int attempt = 0;
      while (!uploadSuccess) {
        try {
          bytesUploaded.addAndGet(uploadFilePartAttempt(attempt++, backupFile, partNumber));
          uploadSuccess = true;
        } catch (IOException e) {
          LOG.error("Upload failed " + e, e);
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

  private long uploadFilePartAttempt(final int attempt, final File backupFile, final int partNumber) throws IOException {
    long uploadLen = backupFile.length();
    // SwiftUtils.debug(LOG, "Uploading part %d of file %s;" + " localfile=%s of length %d - attempt %d", partNumber, key, backupFile, uploadLen, attempt);
    FileInputStream inputStream = null;
    try {
      // this.isDone(results);
      inputStream = new FileInputStream(backupFile);
      nativeStore.uploadFilePart(new Path(key), partNumber, inputStream, uploadLen);
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
    return partNumber.get() - 2;
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
  synchronized long getBytesUploaded() {
    return bytesUploaded.get();
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStream{" + ", key='" + key + '\'' + ", closed=" + closed + ", filePartSize=" + filePartSize + ", blockOffset=" + blockOffset + ", partUpload=" + partUpload
        + ", nativeStore=" + nativeStore + ", bytesWritten=" + bytesWritten + ", bytesUploaded=" + bytesUploaded + '}';
  }
}
