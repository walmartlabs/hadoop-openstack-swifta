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
 * Class that represents authentication request to Openstack Keystone. Contains basic authentication
 * information. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND
 * THEIR ACCESSORS.
 */
public class PasswordAuthenticationRequest extends AuthenticationRequest {

  /**
   * Credentials for login.
   */
  private PasswordCredentials passwordCredentials;

  /**
   * Constructor for password authentication request.
   * 
   * @param tenantName tenant
   * @param passwordCredentials password credentials
   */
  public PasswordAuthenticationRequest(String tenantName, PasswordCredentials passwordCredentials) {
    this.tenantName = tenantName;
    this.passwordCredentials = passwordCredentials;
  }

  /**
   * Get the password credentials.
   * 
   * @return credentials for login into Keystone
   */
  public PasswordCredentials getPasswordCredentials() {
    return passwordCredentials;
  }

  /**
   * Set the password credentials.
   * 
   * @param passwordCredentials credentials for login into Keystone
   */
  public void setPasswordCredentials(PasswordCredentials passwordCredentials) {
    this.passwordCredentials = passwordCredentials;
  }

  @Override
  public String toString() {
    return "Authenticate as tenant '" + Objects.toString(tenantName) + "' "
        + Objects.toString(passwordCredentials);
  }
}
