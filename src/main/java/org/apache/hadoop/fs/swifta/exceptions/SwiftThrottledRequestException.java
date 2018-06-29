/*
 * Copyright (c) [2018]-present, Walmart Inc.
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


package org.apache.hadoop.fs.swifta.exceptions;

import java.net.URI;

import org.apache.commons.httpclient.HttpMethod;

/**
 * Exception raised if a Swift endpoint returned a HTTP response indicating the caller is being
 * throttled.
 */
public class SwiftThrottledRequestException extends SwiftInvalidResponseException {

  private static final long serialVersionUID = 7290021699624281454L;

  public SwiftThrottledRequestException(String message, String operation, URI uri,
      HttpMethod method) {
    super(message, operation, uri, method);
  }
}
