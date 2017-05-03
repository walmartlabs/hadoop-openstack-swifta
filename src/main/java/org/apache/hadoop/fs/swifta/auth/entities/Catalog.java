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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Describes Openstack Swift REST endpoints. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT
 * RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class Catalog {

  /**
   * List of valid swift endpoints.
   */
  private List<Endpoint> endpoints; 
  
  /**
   * Endpoint links are additional information description which aren't used in Hadoop and Swift
   * integration scope.
   */
  @JsonProperty("endpoints_links")
  private List<Object> endpointsLinks;
  
  /**
   * Openstack REST service name. In our case name = "keystone"
   */
  private String name;

  /**
   * Type of REST service. In our case type = "identity"
   */
  private String type;

  /**
   * Get the list of endpoints.
   * 
   * @return List of endpoints
   */
  public List<Endpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * Set the list of endpoints.
   * 
   * @param endpoints list of endpoints
   */
  public void setEndpoints(List<Endpoint> endpoints) {
    this.endpoints = endpoints;
  }

  /**
   * Get the list of endpoint links.
   * 
   * @return list of endpoint links
   */
  public List<Object> getEndpointsLinks() {
    return endpointsLinks;
  }

  /**
   * Set the list of endpoint links.
   * 
   * @param endpointsLinks list of endpoint links
   */
  public void setEndpointsLinks(List<Object> endpointsLinks) {
    this.endpointsLinks = endpointsLinks;
  }

  /**
   * Get the name of Openstack REST service.
   * 
   * @return name of Openstack REST service
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of Openstack REST service.
   * 
   * @param name of Openstack REST service
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get the type of Openstack REST service.
   * 
   * @return type of Openstack REST service
   */
  public String getType() {
    return type;
  }

  /**
   * Set the type of Openstack REST service.
   * 
   * @param type of Openstack REST service
   */
  public void setType(String type) {
    this.type = type;
  }
}
