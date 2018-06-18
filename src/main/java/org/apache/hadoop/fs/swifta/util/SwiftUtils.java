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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;

/**
 * Various utility classes for SwiftFS support.
 */
public final class SwiftUtils {

  private static final String ENCODE = "UTF-8";
  private static final String SLASH = "/";
  private static final String COLON = ":";
  private static final String PLUS = "\\+";
  private static final String PERCENT = "%";
  private static final String PERCENT_ENCODE = "%25";
  private static final String COLON_ENCODE = "%3A";
  private static final String SPACE_ENCODE = "%20";
  public static final String READ = "read(buffer, offset, length)";

  /**
   * Join two (non null) paths, inserting a forward slash between them if needed.
   *
   * @param path1 first path
   * @param path2 second path
   * @return the combined path
   */
  public static String joinPaths(String path1, String path2) {
    StringBuilder result = new StringBuilder(path1.length() + path2.length() + 1);
    result.append(path1);
    boolean insertSlash = true;
    if (path1.endsWith(SLASH)) {
      insertSlash = false;
    } else if (path2.startsWith(SLASH)) {
      insertSlash = false;
    }
    if (insertSlash) {
      result.append(SLASH);
    }
    result.append(path2);
    return result.toString();
  }

  /**
   * This test contains the is-directory logic for Swift, so if changed there is only one place for
   * it.
   *
   * @param fileStatus status to examine
   * @return true if we consider this status to be representative of a directory.
   */
  public static boolean isDirectory(FileStatus fileStatus) {
    return fileStatus.isDirectory() || isFilePretendingToBeDirectory(fileStatus);
  }

  /**
   * Test for the entry being a file that is treated as if it is a directory.
   *
   * @param fileStatus status
   * @return true if it meets the rules for being a directory
   */
  public static boolean isFilePretendingToBeDirectory(FileStatus fileStatus) {
    return fileStatus.getLen() == 0;
  }

  /**
   * Predicate whether a swift object refers to the root directory.
   * 
   * @param swiftObject object to probe
   * @return true iff the object refers to the root
   */
  public static boolean isRootDir(SwiftObjectPath swiftObject) {
    return swiftObject.objectMatches("") || swiftObject.objectMatches(SLASH);
  }

  public static String decodeUrl(String url) throws SwiftException, UnsupportedEncodingException {
    return URLDecoder.decode(url, ENCODE);
  }

  public static String decodeUrlNoCheck(String url) throws SwiftException {
    return url.replace(PERCENT_ENCODE, PERCENT);
  }

  public static String encodeUrlNoCheck(String url) throws SwiftException {
    return url.replace(PLUS, SPACE_ENCODE).replace(COLON, COLON_ENCODE);
  }

  /**
   * Sprintf() to the log iff the log is at debug level. If the log is not at debug level, the
   * printf operation is skipped, so no time is spent generating the string.
   * 
   * @param log log to use
   * @param text text message
   * @param args args arguments to the print statement
   */
  public static void debug(Log log, String text, Object... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(text, args));
    }
  }

  /**
   * Log an exception (in text and trace) iff the log is at debug.
   * 
   * @param log Log to use
   * @param text text message
   * @param ex exception
   */
  public static void debugEx(Log log, String text, Exception ex) {
    if (log.isDebugEnabled()) {
      log.debug(text + ex, ex);
    }
  }

  /**
   * Sprintf() to the log iff the log is at trace level. If the log is not at trace level, the
   * printf operation is skipped, so no time is spent generating the string.
   * 
   * @param log log to use
   * @param text text message
   * @param args args arguments to the print statement
   */
  public static void trace(Log log, String text, Object... args) {
    if (log.isTraceEnabled()) {
      log.trace(String.format(text, args));
    }
  }

  /**
   * Given a partition number, calculate the partition value. This is used in the
   * SwiftNativeOutputStream, and is placed here for tests to be able to calculate the filename of a
   * partition.
   * 
   * @param partNumber part number
   * @return a string to use as the filename
   */
  public static String partitionFilenameFromNumber(int partNumber) {
    return String.format("%06d", partNumber);
  }

  /**
   * List a a path to string.
   * 
   * @param fileSystem filesystem
   * @param path directory
   * @return a listing of the filestatuses of elements in the directory, one to a line, precedeed by
   *         the full path of the directory
   * @throws IOException connectivity problems
   */
  public static synchronized String ls(FileSystem fileSystem, Path path) throws IOException {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top of the path
      return SLASH;
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (NoSuchElementException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return pathtext + fileStatsToString(stats, "\n");
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01] counter.
   * 
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(List<FileStatus> stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.size() * 128);
    for (int i = 0; i < stats.size(); i++) {
      buf.append(String.format("[%02d] %s", i, stats.get(i))).append(separator);
    }
    return buf.toString();
  }

  /**
   * Get the file stats in string.
   * 
   * @param stats the file status
   * @param separator the separator
   * @return the file stats in string
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

  /**
   * Verify that the basic args to a read operation are valid; throws an exception if not -with
   * meaningful text includeing.
   * 
   * @param buffer destination buffer
   * @param off offset
   * @param len number of bytes to read
   * @throws NullPointerException null buffer
   * @throws IndexOutOfBoundsException on any invalid range.
   */
  public static void validateReadArgs(byte[] buffer, int off, int len) {
    if (buffer == null) {
      throw new NullPointerException("Null byte array in" + READ);
    }
    if (off < 0) {
      throw new IndexOutOfBoundsException("Negative buffer offset " + off + " in " + READ);
    }
    if (len < 0) {
      throw new IndexOutOfBoundsException("Negative read length " + len + " in " + READ);
    }
    if (off > buffer.length) {
      throw new IndexOutOfBoundsException(
          "Buffer offset of " + off + "beyond buffer size of " + buffer.length + " in " + READ);
    }
  }
}
