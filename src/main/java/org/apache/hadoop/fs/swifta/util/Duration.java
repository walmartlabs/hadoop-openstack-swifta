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


package org.apache.hadoop.fs.swifta.util;

public class Duration {

  private final long started;
  private long finished;

  public Duration() {
    started = time();
    finished = started;
  }

  private long time() {
    return System.currentTimeMillis();
  }

  public void finished() {
    finished = time();
  }

  public String getDurationString() {
    return humanTime(value());
  }

  /**
   * Get the human time.
   * @param time the time
   * @return the human time
   */
  public static String humanTime(long time) {
    long seconds = (time / 1000);
    long minutes = (seconds / 60);
    return String.format("%d:%02d:%03d", minutes, seconds % 60, time % 1000);
  }

  @Override
  public String toString() {
    return getDurationString();
  }

  public long value() {
    return finished - started;
  }
}
