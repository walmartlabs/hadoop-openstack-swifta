package org.apache.hadoop.fs.swifta.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swifta.exceptions.SwiftMetricWrongParametersException;

import java.io.OutputStream;
import java.util.Map;
import java.util.WeakHashMap;

public class OutputstreamMetrics implements SwiftMetric {
  private static final Log LOG = LogFactory.getLog(OutputstreamMetrics.class);
  private static final Map<OutputStream, String> outputStreams =
      new WeakHashMap<OutputStream, String>();
  private static final int MAX = 500;
  private String name; // Metric name.

  public OutputstreamMetrics(String name) {
    this.name = name;
  }

  @Override
  public void report() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.name() + outputStreams.size());
    }

  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int increase(Object... objects) throws SwiftMetricWrongParametersException {
    if (objects.length != 2)
      throw new SwiftMetricWrongParametersException(
          this.name() + ": Wrong parameters!<String> <InputStream>");
    String path = (String) objects[0];
    OutputStream output = (OutputStream) objects[1];
    outputStreams.put(output, path);
    if (outputStreams.size() > MAX) {
      LOG.warn("You have too many connections!" + outputStreams.size());
    }
    return outputStreams.size();
  }

  @Override
  public int remove(Object... objects) throws SwiftMetricWrongParametersException {
    if (objects.length != 1)
      throw new SwiftMetricWrongParametersException(
          this.name() + "[remove] Wrong parameters! <InputStream>");
    outputStreams.remove(objects[0]);
    return outputStreams.size();
  }

  @Override
  public int count() {
    return outputStreams.size();
  }
}
