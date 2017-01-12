package org.apache.hadoop.fs.swifta.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.exceptions.SwiftMetricWrongParametersException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsFactory {

  private static final Log LOG = LogFactory.getLog(MetricsFactory.class);

  private static Map<String, MetricsFactory> metricsMap =
      new ConcurrentHashMap<String, MetricsFactory>();

  private SwiftMetric metric;

  @SuppressWarnings("rawtypes")
  public static MetricsFactory getMetricsFactory(Class clazz) {
    String name = clazz.getSimpleName();
    if (!metricsMap.containsKey(name)) {
      if ("SwiftNativeInputStream".equals(name)) {
        metricsMap.put(name, new MetricsFactory(
            new InputstreamMetrics("Total opened SwiftNativeInputStream connections to cloud.")));
      } else if ("SwiftNativeFileSystem".equals(name)) {
        metricsMap.put(name,
            new MetricsFactory(new SwiftaFileSystemMetrics("Total swift filesystem instances.")));
      } else if ("SwiftNativeFileSystemStore".equals(name)) {
        metricsMap.put(name, new MetricsFactory(
            new SwiftaFileSystemStoreMetrics("Total swift filesystem store instances.")));
      } else if ("HttpInputStreamWithRelease".equals(name)) {
        metricsMap.put(name, new MetricsFactory(new InputstreamMetrics(
            "Total opened HttpInputStreamWithRelease connections to cloud.")));
      } else if ("SwiftRestClient".equals(name)) {
        metricsMap.put(name,
            new MetricsFactory(new SwiftRestClientMetrics("Total swift rest client instances.")));
      } else if ("SwiftNativeOutputStream".equals(name)) {
        metricsMap.put(name, new MetricsFactory(
            new OutputstreamMetrics("Total opened SwiftNativeOutputStream connections to cloud.")));
      } else {
        throw new UnsupportedOperationException("This method has not supported yet!");
      }
    }
    return metricsMap.get(name);
  }

  private MetricsFactory(SwiftMetric metric) {
    this.metric = metric;
  }

  public void increase(Object... objects) {
    try {
      metric.increase(objects);
    } catch (SwiftMetricWrongParametersException e) {
      LOG.warn("Cannot add metric. " + e.getMessage());
    }
  }

  public void remove(Object... objects) {
    try {
      metric.remove(objects);
    } catch (SwiftMetricWrongParametersException e) {
      LOG.warn("Cannot remove metric. " + e.getMessage());
    }
  }

  public void report() {
    metric.report();
  }
}
