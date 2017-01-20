package org.apache.hadoop.fs.swifta.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadManager {

  private static final Log LOG = LogFactory.getLog(ThreadManager.class);
  private static final int AWAIT_TIMEOUT = 3;

  private ExecutorService threadPool;

  private ThreadPoolExecutor createThreadManager(int coreThreads, int totalThreads,
      ThreadFactory factory) {
    if (factory == null) {
      factory = Executors.defaultThreadFactory();
    }
    return new ThreadPoolExecutor(coreThreads, totalThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), factory);
  }

  public void createThreadManager(int maxThread) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Create pool with threads " + maxThread);
    }
    ThreadPoolExecutor pool = this.createThreadManager(maxThread, maxThread, null);
    pool.allowCoreThreadTimeOut(true);
    threadPool = pool;
  }

  public ExecutorService getPool() {
    return this.threadPool;
  }

  public void shutdown() {
    if (threadPool != null && !threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  public void cleanup() {
    try {
      if (threadPool != null
          && !this.threadPool.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        this.threadPool.shutdownNow();
      }
    } catch (Exception e) {
      // ignore
    }
    threadPool = null;
  }

}
