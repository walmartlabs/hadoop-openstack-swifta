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

package org.apache.hadoop.fs.swifta.util;

public class ThreadUtils {
  /**
   * Timeout for thread to die.
   */
  private static final int TIMEOUT = 1000;

  /**
   * Default max threads.
   */
  private static final int THREADS_PER_PROCESSOR = 6;

  public static boolean terminate(Thread thread) {
    return ThreadUtils.terminate(thread, TIMEOUT);
  }

  /**
   * Terminate a thread.
   * 
   * @param thread Thread to terminate.
   * @param timeoutToDie milliseconds
   * @return Is terminated.
   */
  public static boolean terminate(Thread thread, long timeoutToDie) {
    boolean isDone = false;
    if (!thread.isAlive()) {
      isDone = true;
    }
    thread.interrupt(); // Graceful shutdown
    try {
      thread.join(timeoutToDie);
    } catch (InterruptedException e) {
      // Ignore
    }
    if (!thread.isAlive()) {
      isDone = true;
    }
    thread.setPriority(Thread.MIN_PRIORITY);
    thread = null;
    return isDone;
  }

  public static int getMaxThread() {
    return Runtime.getRuntime().availableProcessors() * THREADS_PER_PROCESSOR;
  }

}

