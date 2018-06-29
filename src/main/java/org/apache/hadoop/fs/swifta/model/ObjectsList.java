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


package org.apache.hadoop.fs.swifta.model;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;

/**
 * List of objects from cloud.
 */
public class ObjectsList {

  private List<FileStatus> files = Collections.emptyList();

  private String marker;

  public String getMarker() {
    return marker;
  }

  public void setMarker(String marker) {
    this.marker = marker;
  }

  public List<FileStatus> getFiles() {
    return files;
  }

  public void setFiles(List<FileStatus> files) {
    this.files = files;
  }

}
