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

import org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Date;

/**
 * Java mapping of Swift JSON file status. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT
 * RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */

class SwiftObjectFileStatus {
  private static final String SLASH = "/";
  private long bytes;
  @JsonProperty("content_type")
  private String contentType;
  private String hash;
  @JsonProperty("last_modified")
  private Date lastModified;
  private String name;
  private String subdir;
  /**
   * This field name maybe changed, hold the position here.
   */
  @JsonProperty("user_custom_data")
  private String fileLen;

  SwiftObjectFileStatus() {}

  SwiftObjectFileStatus(long bytes, String contentType, String hash, Date lastModified,
      String name) {
    this.bytes = bytes;
    this.contentType = contentType;
    this.hash = hash;
    this.lastModified = lastModified;
    this.name = name;
  }

  public long getBytes() {
    return bytes;
  }

  public void setBytes(long bytes) {
    this.bytes = bytes;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public String getHash() {
    return hash;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public Date getLastModified() {
    return lastModified;
  }

  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified == null ? lastModified : new Date(lastModified.getTime());
    // this.lastModified = lastModified;
  }

  public String getName() {
    return pathToRootPath(name);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSubdir() {
    return pathToRootPath(subdir);
  }

  public void setSubdir(String subdir) {
    this.subdir = subdir;
  }

  /**
   * If path doesn't starts with '/' method will concat '/'.
   *
   * @param path specified path
   * @return root path string
   */
  private String pathToRootPath(String path) {
    if (path == null) {
      return null;
    }

    if (path.startsWith(SLASH)) {
      return path;
    }

    return SLASH.concat(path);
  }

  public String getFileLen() {
    return fileLen;
  }

  public void setFileLen(String fileLen) {
    this.fileLen = fileLen;
  }

  public boolean isDir() {
    return SwiftProtocolConstants.CONTENT_TYPE_DIRECTORY.equals(contentType);
  }
}
