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

package org.apache.hadoop.fs.swifta.auth;

/**
 * Describes user roles in Openstack system. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT
 * RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class Roles {
  
  /**
   * The role name.
   */
  private String name;

  /**
   * The field user in the RackSpace auth model.
   */
  private String id;

  /**
   * The field user in the RackSpace auth model.
   */
  private String description;

  /**
   * The service id used in the HP public cloud.
   */
  private String serviceId;

  /**
   * The tenant id used in the HP public cloud.
   */
  private String tenantId;

  /**
   * Get the role name.
   * 
   * @return role name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the role name.
   * 
   * @param name role name
   */
  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getServiceId() {
    return serviceId;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }
}
