package org.apache.hadoop.fs.swifta.http;

import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Better to use a separate thread to check.
 *
 */
public class IdleConnectionMonitorThread extends Thread {

  private final MultiThreadedHttpConnectionManager connMgr;

  private static final Log LOG = LogFactory.getLog(IdleConnectionMonitorThread.class);

  public IdleConnectionMonitorThread(MultiThreadedHttpConnectionManager connMgr) {
    super();
    this.connMgr = connMgr;
  }

  @Override
  public void run() {
    try {
      // Deletes all closed connections.
      connMgr.deleteClosedConnections();
      // Close connections that have been idle longer than 300 seconds.
      connMgr.closeIdleConnections(300000);
      if (LOG.isDebugEnabled()) {
        LOG.debug("The total number of pooled connections " + connMgr.getConnectionsInPool() + " out of " + connMgr.getParams().getMaxTotalConnections() + "; default max connections per host "
            + connMgr.getParams().getDefaultMaxConnectionsPerHost());
      }

    } catch (Exception e) {
      // terminate
    }
  }

}
