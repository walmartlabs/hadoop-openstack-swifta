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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;

/**
 * A subclass of {@link FileStatus} that contains the Swift-specific rules of when a file is considered to be a directory.
 */
public class SwiftFileStatus extends FileStatus {

  private SwiftObjectPath objectManifest = null;

  public SwiftFileStatus() {}

  public SwiftFileStatus(long length, boolean isdir, int blockReplication, long blocksize, long modificationTime, Path path, SwiftObjectPath objectManifest) {
    super(length, isdir, blockReplication, blocksize, modificationTime, path);
    this.objectManifest = objectManifest;
  }

  public SwiftFileStatus(long length, boolean isdir, int blockReplication, long blocksize, long modificationTime, Path path) {
    super(length, isdir, blockReplication, blocksize, modificationTime, path);
  }

  public SwiftFileStatus(long length, boolean isdir, int blockReplication, long blocksize, long modificationTime, long accessTime, FsPermission permission, String owner, String group, Path path) {
    super(length, isdir, blockReplication, blocksize, modificationTime, accessTime, permission, owner, group, path);
  }

  /**
   * Declare that the path represents a directory, which in the SwiftNativeFileSystem means "is a directory or a 0 byte file".
   *
   * @return true if the status is considered to be a file
   */
  @Override
  public boolean isDir() {
    return super.isDir() || getLen() == 0;
  }

  /**
   * A entry is a file if it is not a directory. By implementing it <i>and not marking as an override</i> this subclass builds and runs in both Hadoop versions.
   * 
   * @return the opposite value to {@link #isDir()}
   */
  public boolean isFile() {
    return !isDir();
  }

  /**
   * Directory test.
   * 
   * @return true if the file is considered to be a directory
   */
  public boolean isDirectory() {
    return isDir();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("{ ");
    sb.append("path=").append(getPath());
    sb.append("; isDirectory=").append(isDir());
    sb.append("; length=").append(getLen());
    sb.append("; blocksize=").append(getBlockSize());
    sb.append("; modification_time=").append(getModificationTime());
    sb.append("}");
    return sb.toString();
  }

  public SwiftObjectPath getObjectManifest() {
    return objectManifest;
  }

  public boolean isPartitionedFile() {
    return objectManifest != null;
  }
}
