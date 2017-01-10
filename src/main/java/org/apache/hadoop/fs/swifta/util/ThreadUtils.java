package org.apache.hadoop.fs.swifta.util;

public class ThreadUtils {
  /**
   * Timeout for thread to die.
   */
  private static final int TIMEOUT = 1000;

  public static boolean terminate(Thread thread) {
    return ThreadUtils.terminate(thread, TIMEOUT);
  }

  /**
   * Terminate a thread.
   * 
   * @param t Thread to terminate.
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

}

