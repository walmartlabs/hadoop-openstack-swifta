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
import org.apache.hadoop.fs.swifta.http.SwiftRestClient;

import java.util.Map;
import java.util.WeakHashMap;

public class SwiftRestClientMetrics implements SwiftMetric {
  private static final Log LOG = LogFactory.getLog(SwiftRestClientMetrics.class);
  private static final Map<SwiftRestClient, String> client =
      new WeakHashMap<SwiftRestClient, String>();
  private static final int MAX = 500;
  private String name; // Metric name.

  public SwiftRestClientMetrics(String name) {
    this.name = name;
  }

  @Override
  public void report() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.name() + client.size());
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
          this.name() + ":Wrong parameters!<SwiftRestClient>");
    }
    SwiftRestClient swift = (SwiftRestClient) objects[0];
    client.put(swift, "test");
    if (client.size() > MAX) {
      LOG.warn("You have too many connections!" + client.size());
    }
    return client.size();
  }

  @Override
  public int remove(Object... objects) throws SwiftMetricWrongParametersException {
    if (objects.length != 1) {
      throw new SwiftMetricWrongParametersException(
          this.name() + "[remove]Wrong parameters! <InputStream>");
    }
    client.remove(objects[0]);
    return client.size();
  }

  @Override
  public int count() {
    return client.size();
  }
}
