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

package org.apache.hadoop.fs.swifta.auth.entities;

import org.apache.hadoop.fs.swifta.auth.Roles;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Describes user entity in Keystone In different Swift installations User is represented
 * differently. To avoid any JSON deserialization failures this entity is ignored. THIS FILE IS
 * MAPPED BY JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

  /**
   * The user id in Keystone.
   */
  private String id;

  /**
   * The user human readable name.
   */
  private String name;

  /**
   * The user roles in Keystone.
   */
  private List<Roles> roles;

  /**
   * The links to user roles.
   */
  @JsonProperty("roles_links")
  private List<Object> rolesLinks;

  /**
   * The human readable username in Keystone.
   */
  private String username;

  /**
   * Get the user id.
   * 
   * @return user id
   */
  public String getId() {
    return id;
  }

  /**
   * Set the user id.
   * 
   * @param id user id
   */
  public void setId(String id) {
    this.id = id;
  }


  /**
   * Get the user name.
   * 
   * @return user name
   */
  public String getName() {
    return name;
  }


  /**
   * Set the user name.
   * 
   * @param name user name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get the user roles.
   * 
   * @return user roles
   */
  public List<Roles> getRoles() {
    return roles;
  }

  /**
   * Set the user roles.
   * @param roles sets user roles
   */
  public void setRoles(List<Roles> roles) {
    this.roles = roles;
  }

  /**
   * Get the user roles links.
   * 
   * @return user roles links
   */
  public List<Object> getRolesLinks() {
    return rolesLinks;
  }

  /**
   * Set the user roles links.
   * 
   * @param rolesLinks user roles links
   */
  public void setRolesLinks(List<Object> rolesLinks) {
    this.rolesLinks = rolesLinks;
  }

  /**
   * Get the user name.
   * 
   * @return username
   */
  public String getUsername() {
    return username;
  }

  /**
   * Set the user name.
   * 
   * @param username human readable user name
   */
  public void setUsername(String username) {
    this.username = username;
  }
}
