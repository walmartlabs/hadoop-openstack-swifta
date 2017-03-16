package org.apache.hadoop.fs.swifta.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SwiftClientConfigFactory {


  private final static Map<String, SwiftClientConfig> multiClouds = new ConcurrentHashMap<String, SwiftClientConfig>();


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
