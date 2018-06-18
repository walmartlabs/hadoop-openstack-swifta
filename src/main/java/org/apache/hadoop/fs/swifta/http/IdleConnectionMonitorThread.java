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
        LOG.debug("The total number of pooled connections " + connMgr.getConnectionsInPool()
            + " out of " + connMgr.getParams().getMaxTotalConnections()
            + "; default max connections per host "
            + connMgr.getParams().getDefaultMaxConnectionsPerHost());
      }

    } catch (Exception e) {
      // terminate
    }
  }

}
