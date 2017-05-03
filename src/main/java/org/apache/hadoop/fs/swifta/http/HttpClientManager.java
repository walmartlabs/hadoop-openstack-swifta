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

package org.apache.hadoop.fs.swifta.http;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.util.ThreadUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpClientManager {

  private static final Log LOG = LogFactory.getLog(HttpClientManager.class);
  private static final int INITAL_DELAY = 5;
  private static final int PERIOD = 120;
  private static final String THREAD_NAME = "Swift-Httpclient-Monitor";
  private static MultiThreadedHttpConnectionManager connectionManager = null;
  private static HttpConnectionManagerParams connParam = null;

  private static Object lock = new Object();

  public static MultiThreadedHttpConnectionManager getHttpManager(SwiftClientConfig clientConfig) {
    if (connectionManager == null) {
      synchronized (lock) {
        if (connectionManager == null) {
          initConnectionManager(clientConfig);
        }
      }
    }
    return connectionManager;
  }

  private static void initConnectionManager(SwiftClientConfig clientConfig) {
    connectionManager = new MultiThreadedHttpConnectionManager();
    connParam = new HttpConnectionManagerParams();
    int totalThreads = clientConfig.getMaxTotalConnections();

    /**
     * Httpclient threads as default.
     */
    if (totalThreads < 1) {
      int connections = ThreadUtils.getMaxThread() << 1;
      totalThreads = connections;
    }

    int coreThreads = clientConfig.getMaxCoreConnections();
    if (coreThreads < 1) {
      coreThreads = (totalThreads >> 1);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Max total threads " + totalThreads + "; max core threads " + coreThreads);
    }

    connParam.setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION, coreThreads);
    connParam.setMaxTotalConnections(totalThreads);
    connParam.setSoTimeout(clientConfig.getSocketTimeout());
    connParam.setConnectionTimeout(clientConfig.getConnectTimeout());
    connParam.setTcpNoDelay(Boolean.TRUE);
    connectionManager.setParams(connParam);
    // Connection eviction
    ScheduledExecutorService scheduledExeService = Executors.newScheduledThreadPool(1, new DaemonThreadFactory(THREAD_NAME));
    scheduledExeService.scheduleAtFixedRate(new IdleConnectionMonitorThread(connectionManager), INITAL_DELAY, PERIOD, TimeUnit.SECONDS);
  }
}
