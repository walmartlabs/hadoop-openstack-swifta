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

package org.apache.hadoop.fs.swifta.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SwiftClientConfigFactory {

  private static final Map<String, SwiftClientConfig> multiClouds = new ConcurrentHashMap<String, SwiftClientConfig>();


  public static SwiftClientConfig getInstance(String service, Configuration conf) throws SwiftConfigurationException {
    if (multiClouds.get(service) == null) {
      synchronized (multiClouds) {
        if (multiClouds.get(service) == null) {
          multiClouds.put(service, new SwiftClientConfig(service, conf));
        }
      }
    }
    return multiClouds.get(service);
  }


}
