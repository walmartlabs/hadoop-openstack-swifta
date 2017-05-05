package org.apache.hadoop.fs.swifta.model;

import org.apache.hadoop.fs.FileStatus;

import java.util.Collections;
import java.util.List;

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
