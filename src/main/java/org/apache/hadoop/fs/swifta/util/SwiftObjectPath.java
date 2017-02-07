/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.fs.swifta.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.http.RestClientBindings;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Swift hierarchy mapping of (container, path).
 */
public final class SwiftObjectPath {
  private static final Pattern PATH_PART_PATTERN = Pattern.compile(".*/AUTH_\\w*/");
  private static final String SLASH = "/";

  /**
   * Swift container.
   */
  private final String container;

  /**
   * swift object
   */
  private final String object;

  private final String uriPath;

  /**
   * Build an instance from a (host, object) pair.
   *
   * @param container container name
   * @param object object ref underneath the container
   */
  public SwiftObjectPath(String container, String object) {

    this.container = container;
    this.object = object;
    uriPath = buildUriPath();
  }

  public String getContainer() {
    return container;
  }

  public String getObject() {
    return object;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SwiftObjectPath)) {
      return Boolean.FALSE;
    }
    final SwiftObjectPath that = (SwiftObjectPath) obj;
    return this.toUriPath().equals(that.toUriPath());
  }

  @Override
  public int hashCode() {
    int result = container.hashCode();
    result = 31 * result + object.hashCode();
    return result;
  }

  private String buildUriPath() {
    return SwiftUtils.joinPaths(container, object);
  }

  public String toUriPath() {
    return uriPath;
  }

  @Override
  public String toString() {
    return toUriPath();
  }

  /**
   * Test for the object matching a path, ignoring the container value.
   *
   * @param path path string
   * @return true iff the object's name matches the path
   */
  public boolean objectMatches(String path) {
    return object.equals(path);
  }


  /**
   * Query to see if the possibleChild object is a child path of this. object.
   *
   * The test is done by probing for the path of the this object being at the start of the second -with a trailing slash, and both containers being equal
   *
   * @param possibleChild possible child dir
   * @return true iff the possibleChild is under this object
   */
  public boolean isEqualToOrParentOf(SwiftObjectPath possibleChild) {
    String origPath = toUriPath();
    String path = origPath;
    if (!path.endsWith(SLASH)) {
      path = path + SLASH;
    }
    String childPath = possibleChild.toUriPath();
    return childPath.equals(origPath) || childPath.startsWith(path);
  }

  /**
   * Create a path tuple of (container, path), where the container is chosen from the host of the URI.
   *
   * @param uri uri to start from
   * @param path path underneath
   * @return a new instance.
   * @throws SwiftConfigurationException if the URI host doesn't parse into container.service
   */
  public static SwiftObjectPath fromPath(URI uri, Path path) throws SwiftConfigurationException {
    return fromPath(uri, path, Boolean.FALSE);
  }

  public static SwiftObjectPath fromPath(URI uri, Path path, boolean addTrailingSlash) throws SwiftConfigurationException {
    return fromPath(uri, path, addTrailingSlash, Boolean.FALSE);
  }

  /**
   * Create a path tuple of (container, path), where the container is chosen from the host of the URI. A trailing slash can be added to the path. This is the point where these /-es need to be
   * appended, because when you construct a {@link Path} instance, {@link Path#normalizePath(String, String)} is called -which strips off any trailing slash.
   *
   * @param uri uri to start from
   * @param path path underneath
   * @param addTrailingSlash should a trailing slash be added if there isn't one.
   * @return a new instance.
   * @throws SwiftConfigurationException if the URI host doesn't parse into container.service
   */
  public static SwiftObjectPath fromPath(URI uri, Path path, boolean addTrailingSlash, boolean removeTrailingSlash) throws SwiftConfigurationException {

    String url = path.toUri().getPath().replaceAll(PATH_PART_PATTERN.pattern(), "");
    // add a trailing slash if needed
    if (addTrailingSlash && !url.endsWith(SLASH)) {
      url += SLASH;
    }
    if (removeTrailingSlash) {
      if (!addTrailingSlash && url.endsWith(SLASH)) {
        url = url.substring(0, url.length() - 1);
      }
    }

    String container = uri.getHost();
    if (container == null) {
      // no container, not good: replace with ""
      container = "";
    } else if (container.contains(".")) {
      // its a container.service URI. Strip the container
      container = RestClientBindings.extractContainerName(container);
    }
    return new SwiftObjectPath(container, url);
  }


}
