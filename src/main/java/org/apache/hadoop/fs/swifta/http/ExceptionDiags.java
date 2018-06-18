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


package org.apache.hadoop.fs.swifta.http;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Variant of Hadoop Netutils exception wrapping with URI awareness and available in branch-1 too.
 */
public class ExceptionDiags {
  private static final Log LOG = LogFactory.getLog(ExceptionDiags.class);

  /**
   * The text to point users elsewhere: {@value}.
   */
  private static final String FOR_MORE_DETAILS_SEE = " For more details see:  ";
  /**
   * The text included in wrapped exceptions if the host is null: {@value}.
   */
  public static final String UNKNOWN_HOST = "(unknown)";
  /**
   * Base URL of the Hadoop Wiki: {@value}.
   */
  public static final String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

  /**
   * Take an IOException and a URI, wrap it where possible with something that includes the URI.
   *
   * @param dest target URI
   * @param operation operation
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static IOException wrapException(final String dest, final String operation,
      final IOException exception) {
    String action = operation + " " + dest;
    String xref = null;

    if (exception instanceof ConnectException) {
      xref = "ConnectionRefused";
    } else if (exception instanceof UnknownHostException) {
      xref = "UnknownHost";
    } else if (exception instanceof SocketTimeoutException) {
      xref = "SocketTimeout";
    } else if (exception instanceof NoRouteToHostException) {
      xref = "NoRouteToHost";
    }
    String msg = action + " failed on exception: " + exception;
    if (xref != null) {
      msg = msg + ";" + see(xref);
    }
    return wrapWithMessage(exception, msg);
  }

  private static String see(final String entry) {
    return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
  }

  private static <T extends IOException> T wrapWithMessage(T exception, String msg) {
    Class<? extends Throwable> clazz = exception.getClass();
    try {
      Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
      Throwable cause = ctor.newInstance(msg);
      exception.initCause(cause);
      return exception;
    } catch (Throwable e) {
      LOG.warn("Unable to wrap exception of type " + clazz + ": it has no (String) constructor", e);
      return exception;
    }
  }

}
