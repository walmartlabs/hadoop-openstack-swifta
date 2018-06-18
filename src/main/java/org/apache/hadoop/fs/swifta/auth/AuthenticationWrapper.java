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


package org.apache.hadoop.fs.swifta.auth;

/**
 * This class is used for correct hierarchy mapping of Keystone authentication model and java code
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR
 * ACCESSORS.
 */
public class AuthenticationWrapper {

  /**
   * Authentication response field.
   */
  private AuthenticationResponse access;

  /**
   * Get the access.
   * 
   * @return authentication response
   */
  public AuthenticationResponse getAccess() {
    return access;
  }

  /**
   * Set the access.
   * 
   * @param access sets authentication response
   */
  public void setAccess(AuthenticationResponse access) {
    this.access = access;
  }
}
