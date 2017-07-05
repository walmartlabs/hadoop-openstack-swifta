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

  /**
   * The constructor for BackupFile.
   * 
   * @param file the file
   * @param partNumber the part number
   * @param output the buffered output stream
   */
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

  /**
   * Wait to close.
   */
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
