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


package org.apache.hadoop.fs.swifta.auth.entities;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Tenant is abstraction in Openstack which describes all account information and user privileges in
 * system. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR
 * ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tenant {

  /**
   * The tenant id.
   */
  private String id;

  /**
   * The tenant short description which Keystone returns.
   */
  private String description;

  /**
   * The boolean indicating whether the user account is enabled.
   */
  private boolean enabled;

  /**
   * The tenant human readable name.
   */
  private String name;

  /**
   * Get the tenant name.
   * 
   * @return tenant name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the tenant name.
   * 
   * @param name tenant name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get whether the account is enabled.
   * 
   * @return true if account enabled and false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Set whether the account is enabled.
   * 
   * @param enabled enable or disable
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Get the account description.
   * 
   * @return account short description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Set the account description.
   * 
   * @param description set account description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Get the tenant id.
   * 
   * @return set tenant id
   */
  public String getId() {
    return id;
  }

  /**
   * Set the tenant id.
   * 
   * @param id tenant id
   */
  public void setId(String id) {
    this.id = id;
  }
}
