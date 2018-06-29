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
public class AuthenticationRequest {

  protected String tenantName;

  /**
   * Default constructor.
   */
  public AuthenticationRequest() {}

  /**
   * Get the Tenant Name.
   * 
   * @return tenant name for Keystone authorization
   */
  public String getTenantName() {
    return tenantName;
  }

  /**
   * Set the Tenant Name.
   * 
   * @param tenantName tenant name for authorization
   */
  public void setTenantName(String tenantName) {
    this.tenantName = tenantName;
  }

  @Override
  public String toString() {
    return "AuthenticationRequest{tenantName='" + Objects.toString(tenantName) + '\'' + '}';
  }
}
