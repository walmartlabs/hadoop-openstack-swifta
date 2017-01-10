package org.apache.hadoop.fs.swifta.http;

import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;

/**
 * Better to use a separate thread to check.
 *
 */
public class IdleConnectionMonitorThread extends Thread {

  private final MultiThreadedHttpConnectionManager connMgr;

  public IdleConnectionMonitorThread(MultiThreadedHttpConnectionManager connMgr) {
    super();
    this.connMgr = connMgr;
  }

  @Override
  public void run() {
    try {
      // Deletes all closed connections.
      connMgr.deleteClosedConnections();
      // Close connections that have been idle longer than 30 sec.
      connMgr.closeIdleConnections(30000);

    } catch (Exception e) {
      // terminate
    }
  }

}
