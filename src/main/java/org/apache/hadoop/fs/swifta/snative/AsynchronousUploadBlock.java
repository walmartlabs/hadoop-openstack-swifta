package org.apache.hadoop.fs.swifta.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.util.ThreadManager;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class AsynchronousUploadBlock extends Thread {
  private static final Log LOG = LogFactory.getLog(AsynchronousUploadBlock.class);
  private BlockingQueue<BackupFile> queue;
  private volatile boolean execute;
  private SwiftOutputStream out;
  private volatile boolean isFinished;
  private int maxThreads;
  private volatile boolean isFull = Boolean.FALSE;

  public AsynchronousUploadBlock(BlockingQueue<BackupFile> queue, SwiftOutputStream out, int maxThreads) {
    super();
    this.queue = queue;
    this.execute = Boolean.TRUE;
    this.out = out;
    this.isFinished = Boolean.FALSE;
    this.maxThreads = maxThreads;
  }

  public void close() {
    this.execute = Boolean.FALSE;
    /**
     * Terminal object
     */
    queue.offer(new BackupFile());
    while (!isFinished) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }

  public boolean isFull() {
    return isFull;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void run() {
    ThreadManager tm = null;
    try {
      tm = new ThreadManager();
      tm.createThreadManager(maxThreads);

      List<Future> uploads = null;
      /**
       * Don't trigger background upload.
       */
      BackupFile pFile = null;
      while (this.execute) {
        // i++;
        /**
         * Get current file, but not upload it.
         */
        BackupFile cFile = queue.take();
        /**
         * Only upload if previous file exist to avoid upload unfinished file chunk.
         */
        if (pFile == null) {
          pFile = cFile;
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(pFile.getPartNumber() + ",Start background upload for: " + pFile.getUploadFile().getName() + ";" + pFile.getUploadFile().length());
        }
        if (queue.size() >= maxThreads) {
          this.isFull = Boolean.TRUE;
        } else {
          this.isFull = Boolean.FALSE;
        }
        uploads = out.doUpload(tm, pFile, pFile.getPartNumber());

        if (cFile.getUploadFile() == null) {
          break;
        }
        pFile = cFile;
      }
      if (tm != null) {
        tm.shutdown();
      }
      if (uploads != null) {
        out.waitToFinish(uploads);
        uploads.clear();
      }

    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Error happen during upload, " + e.getMessage());
    } finally {
      if (tm != null) {
        tm.cleanup();
        tm = null;
      }
      this.execute = Boolean.FALSE;
      isFinished = Boolean.TRUE;
    }

  }
}
