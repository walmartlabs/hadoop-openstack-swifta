package org.apache.hadoop.fs.swifta.snative;

import org.apache.hadoop.fs.swifta.exceptions.SwiftUnsupportedFeatureException;

import java.io.OutputStream;

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
}
