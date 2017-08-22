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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftInternalStateException;
import org.apache.hadoop.fs.swifta.metrics.MetricsFactory;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;

/**
 * Output stream, buffers data on local disk. Writes to Swift on the close() method, unless the file
 * is significantly large that it is being written as partitions. In this case, the first partition
 * is written on the first write that puts data over the partition, as may later writes. The close()
 * then causes the final partition to be written, along with a partition manifest.
 */
public class SwiftNativeOutputStreamMultiPartSingleThread extends SwiftOutputStream {
  private static final Log LOG =
      LogFactory.getLog(SwiftNativeOutputStreamMultiPartSingleThread.class);
  private static final MetricsFactory metric =
      MetricsFactory.getMetricsFactory(SwiftNativeOutputStreamMultiPartSingleThread.class);

  public static final int ATTEMPT_LIMIT = 3;
  private long filePartSize;
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
  final byte[] oneByte = new byte[1];

  /**
   * Create an output stream.
   * 
   * @param conf configuration to use
   * @param nativeStore native store to write through
   * @param key the key to write
   * @param partSizeKb the partition size
   * @throws IOException the exception
   */
  public SwiftNativeOutputStreamMultiPartSingleThread(Configuration conf,
      SwiftNativeFileSystemStore nativeStore, String key, long partSizeKb) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.nativeStore = nativeStore;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    this.partNumber = 1;
    this.blockOffset = 0;
    this.filePartSize = partSizeKb << 10;
    metric.increase(key, this);
    metric.report();
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
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
      if (partUpload) {
        partUpload(true);
        nativeStore.createManifestForPartUpload(keypath, this.bytesUploaded);
      } else {
        uploadOnClose(keypath);
      }
    } finally {
      delete(backupFile);
      backupFile = null;
      metric.remove(this);
      metric.report();
    }
    assert backupStream == null : "backup stream has been reopened";
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
        LOG.info("Upload failed " + e, e);
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
      }
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
    SwiftUtils.debug(LOG, " write(offset=%d, len=%d)", offset, len);

    // if the size of file is greater than the partition limit
    while (blockOffset + len >= filePartSize) {
      // - then partition the blob and upload as many partitions
      // are needed.
      // how many bytes to write for this partition.
      int subWriteLen = (int) (filePartSize - blockOffset);
      if (subWriteLen < 0 || subWriteLen > len) {
        throw new SwiftInternalStateException(
            "Invalid subwrite len: " + subWriteLen + " -buffer len: " + len);
      }
      writeToBackupStream(buffer, offset, subWriteLen);
      // move the offset along and length down
      offset += subWriteLen;
      len -= subWriteLen;
      // now upload the partition that has just been filled up
      // (this also sets blockOffset=0)
      partUpload(false);
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
    backupStream.write(buffer, offset, len);
    // increment the counters
    blockOffset += len;
    bytesWritten += len;
  }

  /**
   * Upload a single partition. This deletes the local backing-file, and re-opens it to create a new
   * one.
   * 
   * @param closingUpload is this the final upload of an upload
   * @throws IOException on IO problems
   */
  private void partUpload(boolean closingUpload) throws IOException {
    if (backupStream != null) {
      backupStream.close();
    }

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
          bytesUploaded += uploadFilePartAttempt(attempt);
          uploadSuccess = true;
        } catch (IOException e) {
          LOG.info("Upload failed " + e, e);
          if (attempt > ATTEMPT_LIMIT) {
            throw e;
          }
        }
      }
      delete(backupFile);
      partNumber++;
      blockOffset = 0;
      if (!closingUpload) {
        // if not the final upload, create a new output stream
        backupFile = newBackupFile();
        backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
      }
    }
  }

  private long uploadFilePartAttempt(int attempt) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG,
        "Uploading part %d of file %s;" + " localfile=%s of length %d  - attempt %d", partNumber,
        key, backupFile, uploadLen, attempt);
    FileInputStream inputStream = null;
    try {
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
   * number of bytes left to upload.
   * 
   * @return the number of bytes written to the remote endpoint
   */
  @Override
  public long getBytesUploaded() {
    return bytesUploaded;
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStreamNoMultiPart{" + ", key='" + key + '\'' + ", backupFile="
        + backupFile + ", closed=" + closed + ", filePartSize=" + filePartSize + ", partNumber="
        + partNumber + ", blockOffset=" + blockOffset + ", partUpload=" + partUpload
        + ", nativeStore=" + nativeStore + ", bytesWritten=" + bytesWritten + ", bytesUploaded="
        + bytesUploaded + '}';
  }
}
