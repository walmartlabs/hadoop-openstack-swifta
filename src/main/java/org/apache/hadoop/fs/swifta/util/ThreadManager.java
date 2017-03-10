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

  public ThreadManager() {}

  public ThreadManager(int minPoolSize, int maxPoolSize) {
    this.createThreadManager(minPoolSize, maxPoolSize);
  }

  private ThreadPoolExecutor createThreadManager(int coreThreads, int totalThreads, ThreadFactory factory) {
    if (factory == null) {
      factory = Executors.defaultThreadFactory();
    }
    return new ThreadPoolExecutor(coreThreads, totalThreads, 120, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), factory);
  }

  public void createThreadManager(int maxPoolSize) {
    maxPoolSize = this.getRightThread(maxPoolSize);
    this.createThreadManager(maxPoolSize, maxPoolSize);
  }

  private int getRightThread(int maxPoolSize) {
    if (maxPoolSize < 1) {
      maxPoolSize = ThreadUtils.getMaxThread();
    }
    return maxPoolSize;
  }

  private void createThreadManager(int minPoolSize, int maxPoolSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Max threads in pool is " + maxPoolSize + ", min threads in pool is " + minPoolSize);
    }
    ThreadPoolExecutor pool = this.createThreadManager(minPoolSize, maxPoolSize, null);
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
      if (threadPool != null && !this.threadPool.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        this.threadPool.shutdownNow();
      }
    } catch (Exception e) {
      // ignore
    }
    threadPool = null;
  }

}
