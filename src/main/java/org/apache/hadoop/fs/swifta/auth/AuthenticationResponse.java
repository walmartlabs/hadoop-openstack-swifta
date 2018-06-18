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

import java.util.List;

import org.apache.hadoop.fs.swifta.auth.entities.AccessToken;
import org.apache.hadoop.fs.swifta.auth.entities.Catalog;
import org.apache.hadoop.fs.swifta.auth.entities.User;

/**
 * Response from KeyStone deserialized into AuthenticationResponse class. THIS FILE IS MAPPED BY
 * JACKSON TO AND FROM JSON. DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class AuthenticationResponse {
  private Object metadata;
  private List<Catalog> serviceCatalog;
  private User user;
  private AccessToken token;

  public Object getMetadata() {
    return metadata;
  }

  public void setMetadata(Object metadata) {
    this.metadata = metadata;
  }

  public List<Catalog> getServiceCatalog() {
    return serviceCatalog;
  }

  public void setServiceCatalog(List<Catalog> serviceCatalog) {
    this.serviceCatalog = serviceCatalog;
  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }

  public AccessToken getToken() {
    return token;
  }

  public void setToken(AccessToken token) {
    this.token = token;
  }
}
