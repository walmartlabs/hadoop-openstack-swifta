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


package org.apache.hadoop.fs.swifta.snative;

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.swifta.exceptions.SwiftUnsupportedFeatureException;
import org.apache.hadoop.fs.swifta.util.ThreadManager;

/**
 * All the swift output stream must extend this class.
 *
 */
public abstract class SwiftOutputStream extends OutputStream {

  private static final String MSG_NOT_SUPPORT = "Not supported.";

  public long getFilePartSize() throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  public synchronized int getPartitionsWritten() throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  public long getBytesWritten() throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  public long getBytesUploaded() throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  @SuppressWarnings("rawtypes")
  public List<Future> doUpload(final ThreadManager tm, final BackupFile uploadFile,
      final int partNumber) throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  @SuppressWarnings("rawtypes")
  public boolean waitToFinish(List<Future> tasks) throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

}
