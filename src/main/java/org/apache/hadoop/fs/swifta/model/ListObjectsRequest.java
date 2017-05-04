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

package org.apache.hadoop.fs.swifta.model;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystemStore;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;

public class ListObjectsRequest {

  private final SwiftObjectPath path;
  private final boolean listDeep;
  private final boolean newest;

  private final SwiftNativeFileSystemStore store;

  private ObjectsList objects;
  private volatile boolean hasRun = false;

  /**
   * The constructor for the ListObjectsRequest.
   * @param path the swift object path
   * @param listDeep whether listing the deep directory
   * @param newest whether to retrieve the newest
   * @param store the file system store
   */
  public ListObjectsRequest(SwiftObjectPath path, boolean listDeep, boolean newest,
      SwiftNativeFileSystemStore store) {
    this.path = path;
    this.listDeep = listDeep;
    this.newest = newest;
    this.store = store;
  }

  /**
   * The constructor for the ListObjectsRequest.
   * @param path the path
   * @param listDeep whether listing the deep directory
   * @param newest whether to retrieve the newest
   * @param store the file system store
   * @throws SwiftConfigurationException the configuration exception
   * @throws SwiftException the swift exception
   */
  public ListObjectsRequest(Path path, boolean listDeep, boolean newest,
      SwiftNativeFileSystemStore store) throws SwiftConfigurationException, SwiftException {
    this.path = store.toDirPath(path);
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
        objects = store.listDirectory(path, listDeep, newest,
            (objects == null ? null : objects.getMarker()));
      } catch (IOException e) {
        /**
         * Fake an exception to capture the IO error.
         */
        NoSuchElementException e1 = new NoSuchElementException();
        e1.initCause(e);
        throw e1;
      } finally {
        if (!hasRun) {
          hasRun = Boolean.TRUE;
        }
      }
      return objects;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove method not supported.");
    }

  }
}
