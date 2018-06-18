/*
 * Copyright (c) [2011]-present, Walmart Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
   * @param objects the Objects
   * @return the instance number
   * @throws SwiftMetricWrongParametersException exception
   */
  public int increase(Object... objects) throws SwiftMetricWrongParametersException;

  /**
   * Decrease the instance number.
   * 
   * @param objects the objects
   * @return the removed instance number
   * @throws SwiftMetricWrongParametersException exception
   */
  public int remove(Object... objects) throws SwiftMetricWrongParametersException;

  /**
   * Total number.
   * 
   * @return the count
   */
  public int count();
}
