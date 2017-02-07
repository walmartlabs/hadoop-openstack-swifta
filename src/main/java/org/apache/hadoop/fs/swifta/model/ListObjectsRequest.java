package org.apache.hadoop.fs.swifta.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystemStore;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ListObjectsRequest {

  private static final Log LOG = LogFactory.getLog(ListObjectsRequest.class);
  private final SwiftObjectPath path;
  private final boolean listDeep;
  private final boolean newest;

  private final SwiftNativeFileSystemStore store;

  private ObjectsList objects;
  private volatile boolean hasRun = false;

  public ListObjectsRequest(SwiftObjectPath path, boolean listDeep, boolean newest, SwiftNativeFileSystemStore store) {
    this.path = path;
    this.listDeep = listDeep;
    this.newest = newest;
    this.store = store;
  }

  public ListObjectsRequest(Path path, boolean listDeep, boolean newest, SwiftNativeFileSystemStore store) throws SwiftConfigurationException, SwiftException {
    this.path = store.toDirPath(store.getCorrectSwiftPath(path));
    this.listDeep = listDeep;
    this.newest = newest;
    this.store = store;
  }

  public Iterator<ObjectsList> iterator() {
    return new ObjectsListIterator();
  }

  class ObjectsListIterator implements Iterator<ObjectsList> {

    @Override
    public boolean hasNext() {
      if (hasRun && objects != null && objects.getMarker() == null) {
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    }

    @Override
    public ObjectsList next() {
      if (!this.hasNext()) {
        return null;
      }
      try {
        String marker = (objects == null ? null : objects.getMarker());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Get objects list from iterator, marker is " + marker);
        }
        objects = store.listDirectory(path, listDeep, newest, marker);
      } catch (IOException e) {
        /**
         * Fake an exception to capture the IO error.
         */
        NoSuchElementException e1 = new NoSuchElementException();
        e1.initCause(e);
        throw e1;
      }
      if (!hasRun) {
        hasRun = Boolean.TRUE;
      }
      return objects;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove method not supported.");
    }

  }
}