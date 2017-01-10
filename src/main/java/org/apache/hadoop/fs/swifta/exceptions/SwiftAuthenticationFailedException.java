/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.swifta.exceptions;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;

import java.net.URI;

/**
 * An exception raised when an authentication request was rejected
 */
public class SwiftAuthenticationFailedException extends SwiftInvalidResponseException {

  private static final long serialVersionUID = -1077000060620644890L;
  private final static String AUTH = "authentication";

  public SwiftAuthenticationFailedException(String message, int statusCode, String operation,
      URI uri) {
    super(message, statusCode, operation, uri);
  }

  public SwiftAuthenticationFailedException(String message, String operation, URI uri,
      HttpMethod method) {
    super(message, operation, uri, method);
  }

  public SwiftAuthenticationFailedException(String message) {
    super(message, HttpStatus.SC_UNAUTHORIZED, AUTH, null);
  }

  @Override
  public String exceptionTitle() {
    return "Authentication Failure";
  }
}