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


package org.apache.hadoop.fs.swifta.auth;

import java.util.Objects;

/**
 * Describes credentials to log in Swift using Keystone authentication. THIS FILE IS MAPPED BY
 * JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class ApiKeyCredentials {

  /**
   * The user login.
   */
  private String username;

  /**
   * The API key.
   */
  private String apikey;

  /**
   * Default constructor.
   */
  public ApiKeyCredentials() {}

  /**
   * The constructor for API key credentials.
   * 
   * @param username user login
   * @param apikey user api key
   */
  public ApiKeyCredentials(String username, String apikey) {
    this.username = username;
    this.apikey = apikey;
  }

  /**
   * Get the API key.
   * 
   * @return user api key
   */
  public String getApiKey() {
    return apikey;
  }

  /**
   * Set the API key.
   * 
   * @param apikey user api key
   */
  public void setApiKey(String apikey) {
    this.apikey = apikey;
  }

  /**
   * Get the user login.
   * 
   * @return login
   */
  public String getUsername() {
    return username;
  }

  /**
   * Set the user login.
   * 
   * @param username login
   */
  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public String toString() {
    return "user '" + Objects.toString(username) + '\'' + " with key of length "
        + ((apikey == null) ? 0 : apikey.length());
  }
}
