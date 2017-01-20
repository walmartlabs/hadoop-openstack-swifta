package org.apache.hadoop.fs.swifta.http;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.util.ThreadUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class HttpClientManager {

  private static final Log LOG = LogFactory.getLog(HttpClientManager.class);
  private static final int INITAL_DELAY = 5;
  private static final int PERIOD = 300;
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
     * Get six times httpclient threads as default.
     */
    int connections = ThreadUtils.getMaxThread() * 6;
    if (totalThreads < 1) {
      totalThreads = connections;
    }

    int coreThreads = clientConfig.getMaxCoreConnections();
    if (coreThreads < 1) {
      coreThreads = connections;
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
    ScheduledExecutorService scheduledExeService =
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory(THREAD_NAME));
    scheduledExeService.scheduleAtFixedRate(new IdleConnectionMonitorThread(connectionManager),
        INITAL_DELAY, PERIOD, TimeUnit.SECONDS);
  }
}


class DaemonThreadFactory implements ThreadFactory {
  private final String name;

  public DaemonThreadFactory(String name) {
    this.name = name;
  }

  public DaemonThreadFactory() {
    this(null);
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = Executors.defaultThreadFactory().newThread(runnable);
    thread.setDaemon(true);
    if (name != null)
      thread.setName(name);
    return thread;
  }
}
