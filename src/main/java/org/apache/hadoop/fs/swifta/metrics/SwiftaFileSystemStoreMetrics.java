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

package org.apache.hadoop.fs.swifta.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.exceptions.SwiftMetricWrongParametersException;
import org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystemStore;

import java.util.Map;
import java.util.WeakHashMap;

public class SwiftaFileSystemStoreMetrics implements SwiftMetric {

  private static final Log LOG = LogFactory.getLog(SwiftaFileSystemStoreMetrics.class);
  private static final Map<SwiftNativeFileSystemStore, String> fileSystem =
      new WeakHashMap<SwiftNativeFileSystemStore, String>();
  private static final int MAX = 500;
  private String name; // Metric name.

  public SwiftaFileSystemStoreMetrics(String name) {
    this.name = name;
  }

  @Override
  public void report() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.name() + fileSystem.size());
    }

  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int increase(Object... objects) throws SwiftMetricWrongParametersException {
    if (objects.length != 1) {
      throw new SwiftMetricWrongParametersException(
          this.name() + ": Wrong parameters!<SwiftaFileSystemStoreMetrics>");
    }
    SwiftNativeFileSystemStore file = (SwiftNativeFileSystemStore) objects[0];
    fileSystem.put(file, "test");
    if (fileSystem.size() > MAX) {
      LOG.warn("You have too many SwiftaFileSystemStore instances!" + fileSystem.size());
    }
    return fileSystem.size();
  }

  @Override
  public int remove(Object... objects) throws SwiftMetricWrongParametersException {
    if (objects.length != 1) {
      throw new SwiftMetricWrongParametersException(
          this.name() + " [remove]Wrong parameters! <SwiftaFileSystemStoreMetrics>");
    }
    fileSystem.remove(objects[0]);
    return fileSystem.size();
  }

  @Override
  public int count() {
    return fileSystem.size();
  }

}
