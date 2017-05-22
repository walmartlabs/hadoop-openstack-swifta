package org.apache.hadoop.fs.swifta.snative;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.Serializable;

public class BackupFile implements Serializable {

  private static final long serialVersionUID = 2053872137059272405L;
  private File file;
  private int partNumber;
  private BufferedOutputStream output;
  private volatile boolean isClosed;
  Object lock = new Object();

  public BackupFile() {
    this.file = null;
  }

  public BackupFile(File file, int partNumber, BufferedOutputStream output) {
    this.file = file;
    this.partNumber = partNumber;
    this.output = output;
  }

  public File getUploadFile() {
    return this.file;
  }

  public BufferedOutputStream getOutput() {
    return this.output;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void setClosed(boolean isClosed) {
    this.isClosed = isClosed;
  }

  public void waitToClose() {

    synchronized (lock) {
      while (isClosed) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
