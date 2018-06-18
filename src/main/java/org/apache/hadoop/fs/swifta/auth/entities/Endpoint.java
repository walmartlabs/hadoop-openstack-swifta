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

import java.net.URI;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Openstack Swift endpoint description. THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON. DO NOT
 * RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class Endpoint {

  /**
   * The endpoint id.
   */
  private String id;

  /**
   * Keystone admin URL.
   */
  private URI adminURL;

  /**
   * Keystone internal URL.
   */
  private URI internalURL;

  /**
   * The public accessible URL.
   */
  private URI publicURL;

  /**
   * The public accessible URL#2.
   */
  private URI publicURL2;

  /**
   * The Openstack region name.
   */
  private String region;

  /**
   * The field tenantId used in the RackSpace authentication model.
   */
  private String tenantId;

  /**
   * This field versionId in the RackSpace authentication model.
   */
  private String versionId;

  /**
   * This field versionInfo in the RackSpace authentication model.
   */
  private String versionInfo;

  /**
   * This field user in the RackSpace authentication model.
   */
  private String versionList;


  /**
   * Get the endpoint id.
   * 
   * @return endpoint id
   */
  public String getId() {
    return id;
  }

  /**
   * Set the endpoint id.
   * 
   * @param id endpoint id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Get the Keystone admin URL.
   * 
   * @return Keystone admin URL
   */
  public URI getAdminURL() {
    return adminURL;
  }

  /**
   * Set the Keystone admin URL.
   * 
   * @param adminURL Keystone admin URL
   */
  public void setAdminURL(URI adminURL) {
    this.adminURL = adminURL;
  }

  /**
   * Get the internal Keystone URL.
   * 
   * @return internal Keystone URL
   */
  public URI getInternalURL() {
    return internalURL;
  }

  /**
   * Set the internal Keystone URL.
   * 
   * @param internalURL Keystone internal URL
   */
  public void setInternalURL(URI internalURL) {
    this.internalURL = internalURL;
  }

  /**
   * Get the public accessible URL.
   * 
   * @return public accessible URL
   */
  public URI getPublicURL() {
    return publicURL;
  }

  /**
   * Set the public accessible URL.
   * 
   * @param publicURL public URL
   */
  public void setPublicURL(URI publicURL) {
    this.publicURL = publicURL;
  }

  public URI getPublicURL2() {
    return publicURL2;
  }

  public void setPublicURL2(URI publicURL2) {
    this.publicURL2 = publicURL2;
  }

  /**
   * Get the Openstack region name.
   * 
   * @return Openstack region name
   */
  public String getRegion() {
    return region;
  }

  /**
   * Set the Openstack region name.
   * 
   * @param region Openstack region name
   */
  public void setRegion(String region) {
    this.region = region;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getVersionId() {
    return versionId;
  }

  public void setVersionId(String versionId) {
    this.versionId = versionId;
  }

  public String getVersionInfo() {
    return versionInfo;
  }

  public void setVersionInfo(String versionInfo) {
    this.versionInfo = versionInfo;
  }

  public String getVersionList() {
    return versionList;
  }

  public void setVersionList(String versionList) {
    this.versionList = versionList;
  }
}
