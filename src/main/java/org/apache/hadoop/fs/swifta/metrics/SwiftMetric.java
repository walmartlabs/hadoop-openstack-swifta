package org.apache.hadoop.fs.swifta.metrics;

import org.apache.hadoop.fs.swifta.exceptions.SwiftMetricWrongParametersException;

public interface SwiftMetric {

  /**
   * Report the status.
   */
  public void report();

  /**
   * Get the name.
   * 
   * @return The name of the target metric.
   */
  public String name();

  /**
   * Increase the instance number.
   * 
   * @param objects
   * @return
   */
  public int increase(Object... objects) throws SwiftMetricWrongParametersException;

  /**
   * Decrease the instance number.
   * 
   * @param objects
   * @return
   */
  public int remove(Object... objects) throws SwiftMetricWrongParametersException;

  /**
   * Total number.
   * 
   * @return
   */
  public int count();
}
