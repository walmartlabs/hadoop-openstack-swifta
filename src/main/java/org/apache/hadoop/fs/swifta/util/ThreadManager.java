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


package org.apache.hadoop.fs.swifta.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ThreadManager {

  private static final Log LOG = LogFactory.getLog(ThreadManager.class);
  private static final int AWAIT_TIMEOUT = 3;

  private ExecutorService threadPool;

  public ThreadManager() {}

  public ThreadManager(int minPoolSize, int maxPoolSize) {
    this.createThreadManager(minPoolSize, maxPoolSize);
  }

  public ThreadManager(int minPoolSize, int maxPoolSize, boolean useMaxPriority) {
    threadPool = this.createThreadManager(minPoolSize, maxPoolSize, new PriorityThreadFactory());
  }

  private ThreadPoolExecutor createThreadManager(int coreThreads, int totalThreads,
      ThreadFactory factory) {
    if (factory == null) {
      factory = Executors.defaultThreadFactory();
    }
    return new ThreadPoolExecutor(coreThreads, totalThreads, 120, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), factory);
  }

  public void createThreadManager(int maxPoolSize) {
    maxPoolSize = this.getRightThread(maxPoolSize);
    this.createThreadManager(maxPoolSize, maxPoolSize);
  }

  private void createThreadManager(int minPoolSize, int maxPoolSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Max threads in pool is " + maxPoolSize + ", min threads in pool is " + minPoolSize);
    }
    ThreadPoolExecutor pool = this.createThreadManager(minPoolSize, maxPoolSize, null);
    pool.allowCoreThreadTimeOut(true);
    threadPool = pool;
  }

  private int getRightThread(int maxPoolSize) {
    if (maxPoolSize < 1) {
      maxPoolSize = ThreadUtils.getMaxThread();
    }
    return maxPoolSize;
  }

  public ExecutorService getPool() {
    return this.threadPool;
  }

  /**
   * Shut down the thread pool. 
   */
  public void shutdown() {
    if (threadPool != null && !threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  /**
   * Clean up the thread pool.
   */
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
