package org.apache.hadoop.fs.swifta.snative;

import org.apache.hadoop.fs.swifta.exceptions.SwiftUnsupportedFeatureException;
import org.apache.hadoop.fs.swifta.util.ThreadManager;

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Future;

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
  public List<Future> doUpload(final ThreadManager tm, final BackupFile uploadFile, final int partNumber) throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

  @SuppressWarnings("rawtypes")
  public boolean waitToFinish(List<Future> tasks) throws SwiftUnsupportedFeatureException {
    throw new SwiftUnsupportedFeatureException(MSG_NOT_SUPPORT);
  }

}
