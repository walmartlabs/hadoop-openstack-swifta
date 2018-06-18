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
 * This class is used for correct hierarchy mapping of Keystone authentication model and java code.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR
 * ACCESSORS.
 */
public class AuthenticationRequestWrapper {
  
  /**
   * Authentication request.
   */
  private AuthenticationRequest auth; 

  /**
   * Default constructor.
   */
  public AuthenticationRequestWrapper() {}

  /**
   * Constructor for AuthenticationRequestWrapper.
   * 
   * @param auth authentication requests
   */
  public AuthenticationRequestWrapper(AuthenticationRequest auth) {
    this.auth = auth;
  }

  /**
   * Get the authentication request.
   * 
   * @return authentication request
   */
  public AuthenticationRequest getAuth() {
    return auth;
  }

  /**
   * Set the authentication request.
   * 
   * @param auth authentication request
   */
  public void setAuth(AuthenticationRequest auth) {
    this.auth = auth;
  }
}
