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
   * +1 the total count.
   * 
   * @param objects
   * @return
   */
  public int increase(Object... objects) throws SwiftMetricWrongParametersException;

  /**
   * -1 the total count.
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
