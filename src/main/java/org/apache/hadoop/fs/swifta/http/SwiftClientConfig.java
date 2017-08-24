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

package org.apache.hadoop.fs.swifta.http;

import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_CONNECT_TIMEOUT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_RETRY_AUTH_COUNT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_RETRY_COUNT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_SWIFT_BLOCKSIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_SWIFT_INPUT_STREAM_BUFFER_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_SWIFT_PARTITION_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_SWIFT_REQUEST_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_THROTTLE_DELAY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.DEFAULT_WRITE_POLICY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.LRU_LIVE_TIME;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.LRU_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_APIKEY_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_AUTH_ENDPOINT_PREFIX;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_AUTH_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_BLOCKSIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_CONNECTION_TIMEOUT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_CONTAINER_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_INPUT_STREAM_BUFFER_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_LAZY_SEEK;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_LOCATION_AWARE_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_CONNECTIONS_FOR_COPY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_CONNECTIONS_FOR_DELETE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_CONNECTIONS_FOR_UPLOAD;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_HOST_CONNECTIONS;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_TOTAL_CONNECTIONS;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_OUTPUT_STREAM_BUFFER_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_PARTITION_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_PASSWORD_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_PROXY_HOST_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_PROXY_PORT_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_PUBLIC_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_REGION_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_REQUEST_SIZE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_RETRY_AUTH_COUNT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_RETRY_COUNT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_SERVICE_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_TENANT_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_THROTTLE_DELAY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_USERNAME_PROPERTY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_WRITE_POLICY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.USE_HEADER_CACHE;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swifta.auth.ApiKeyAuthenticationRequest;
import org.apache.hadoop.fs.swifta.auth.ApiKeyCredentials;
import org.apache.hadoop.fs.swifta.auth.AuthenticationRequest;
import org.apache.hadoop.fs.swifta.auth.KeyStoneAuthRequest;
import org.apache.hadoop.fs.swifta.auth.KeystoneApiKeyCredentials;
import org.apache.hadoop.fs.swifta.auth.PasswordAuthenticationRequest;
import org.apache.hadoop.fs.swifta.auth.PasswordCredentials;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.WritePolicies;
import org.apache.hadoop.fs.swifta.util.DurationStatsTable;


public class SwiftClientConfig {

  private static final Log LOG = LogFactory.getLog(SwiftClientConfig.class);

  private static final int DEFAULT_CONNECTIONS = 0; // Default set to 0.

  private static final int DEFAULT_COPY_CONNECTIONS = 0; // Default set to 0.

  private static final int DEFAULT_LRU_SIZE = 1000;

  /**
   * In milliseconds.
   */
  private static final long DEFAULT_EXPIRES_TIME = 30 * 60 * 1000;

  /**
   * The authentication endpoint as supplied in the configuration.
   */
  private URI authUri;

  /**
   * Swift region. Some OpenStack installations has more than one region. In this case user can
   * specify the region with which Hadoop will be working.
   */
  private String region;

  /**
   * The tenant name.
   */
  private String tenant;

  /**
   * The user name.
   */
  private String username;

  /**
   * The user password.
   */
  private String password;

  /**
   * The user API key.
   */
  private String apiKey;

  /**
   * The authentication request used to authenticate with Swift.
   */
  private AuthenticationRequest authRequest;

  /**
   * This auth request is similar to @see authRequest, with one difference: it has another json
   * representation when authRequest one is not applicable.
   */
  private AuthenticationRequest keystoneAuthRequest;

  private String serviceDescription;

  private URI filesystemUri;

  /**
   * The name of the service provider.
   */
  private String serviceProvider;

  /**
   * Should the public swift endpoint be used rather than the in-cluster one.
   */
  private boolean usePublicUrl;

  /**
   * Number of times to retry a connection.
   */
  private int retryCount;

  /**
   * Number of times to retry a auth.
   */
  private int retryAuth;

  /**
   * Entry in the swift catalog defining the prefix used to talk to objects.
   */
  private String authEndpointPrefix;

  /**
   * How long (in milliseconds) should a connection be attempted.
   */
  private int connectTimeout;

  /**
   * How long (in milliseconds) should a connection be attempted.
   */
  private int socketTimeout;

  private String writePolicy;

  /**
   * How long (in milliseconds) between bulk operations.
   */
  private int throttleDelay;

  /**
   * The name of a proxy host (can be null, in which case there is no proxy).
   */
  private String proxyHost;

  /**
   * The port of a proxy. This is ignored if {@link #proxyHost} is null.
   */
  private int proxyPort;

  /**
   * Flag to indicate whether or not the client should query for file location data.
   */
  private boolean locationAware;

  private long partSizeKb;

  private long partSizeBytes;

  /**
   * The blocksize of this FS.
   */
  private int blocksizeKb;

  private int bufferSizeKb;

  private int maxCoreConnections;

  private int maxTotalConnections;

  private int inputBufferSize;

  private int outputBufferSize;

  private boolean isLazySeek;

  /**
   * The LRU cache size for the HEAD requests.
   */
  private int lruCacheSize;

  /**
   * The time the LRU cache lives.
   */
  private long cacheLiveTime;

  private boolean useHeaderCache;

  private DurationStatsTable durationStats = new DurationStatsTable();

  private String service;

  private Configuration conf;

  private Properties props;

  /**
   * Max threads for thread manager.
   */
  private int maxThreadsInPool;

  /**
   * Max threads for copy tasks.
   */
  private int maxThreadsForCopy;

  /**
   * Max threads for upload.
   */
  private int maxInParallelUpload;


  /**
   * Create a Swift Client configuration instance.
   *
   * @param service which cloud.
   * @param conf The configuration to use to extract the binding.
   * @throws SwiftConfigurationException the configuration is not valid for defining a rest client
   *         against the service.
   */
  public SwiftClientConfig(String service, Configuration conf) throws SwiftConfigurationException {
    this.service = service;
    this.conf = conf;
    this.loadProperties();
  }

  private void loadProperties() throws SwiftConfigurationException {
    props = RestClientBindings.bind(service, conf);
    String stringAuthUri = getOption(props, SWIFT_AUTH_PROPERTY);
    username = getOption(props, SWIFT_USERNAME_PROPERTY);
    password = props.getProperty(SWIFT_PASSWORD_PROPERTY);
    apiKey = props.getProperty(SWIFT_APIKEY_PROPERTY);
    // optional
    region = props.getProperty(SWIFT_REGION_PROPERTY);
    // tenant is optional
    tenant = props.getProperty(SWIFT_TENANT_PROPERTY);
    // service is used for diagnostics
    serviceProvider = props.getProperty(SWIFT_SERVICE_PROPERTY);
    String isPubProp = props.getProperty(SWIFT_PUBLIC_PROPERTY, "false");
    usePublicUrl = "true".equals(isPubProp);
    authEndpointPrefix = getOption(props, SWIFT_AUTH_ENDPOINT_PREFIX);
    maxCoreConnections = conf.getInt(SWIFT_MAX_HOST_CONNECTIONS, DEFAULT_CONNECTIONS);

    maxTotalConnections = conf.getInt(SWIFT_MAX_TOTAL_CONNECTIONS, DEFAULT_CONNECTIONS);

    lruCacheSize = conf.getInt(LRU_SIZE, DEFAULT_LRU_SIZE);
    this.cacheLiveTime = conf.getLong(LRU_LIVE_TIME, DEFAULT_EXPIRES_TIME);
    // Default set to false.
    isLazySeek = conf.getBoolean(SWIFT_LAZY_SEEK, Boolean.FALSE);
    useHeaderCache = conf.getBoolean(USE_HEADER_CACHE, Boolean.FALSE);

    maxThreadsInPool = conf.getInt(SWIFT_MAX_CONNECTIONS_FOR_DELETE, DEFAULT_CONNECTIONS);

    maxThreadsForCopy = conf.getInt(SWIFT_MAX_CONNECTIONS_FOR_COPY, DEFAULT_COPY_CONNECTIONS);

    maxInParallelUpload = conf.getInt(SWIFT_MAX_CONNECTIONS_FOR_UPLOAD, DEFAULT_CONNECTIONS);

    if (apiKey == null && password == null) {
      throw new SwiftConfigurationException("Configuration for " + filesystemUri
          + " must contain either " + SWIFT_PASSWORD_PROPERTY + " or " + SWIFT_APIKEY_PROPERTY);
    }
    // create the (reusable) authentication request
    if (password != null) {
      authRequest =
          new PasswordAuthenticationRequest(tenant, new PasswordCredentials(username, password));
    } else {
      authRequest =
          new ApiKeyAuthenticationRequest(tenant, new ApiKeyCredentials(username, apiKey));
      keystoneAuthRequest =
          new KeyStoneAuthRequest(tenant, new KeystoneApiKeyCredentials(username, apiKey));
    }
    locationAware = "true".equals(props.getProperty(SWIFT_LOCATION_AWARE_PROPERTY, "false"));

    // now read in properties that are shared across all connections

    // connection and retries
    try {
      retryCount = conf.getInt(SWIFT_RETRY_COUNT, DEFAULT_RETRY_COUNT);
      retryAuth = conf.getInt(SWIFT_RETRY_AUTH_COUNT, DEFAULT_RETRY_AUTH_COUNT);
      connectTimeout = conf.getInt(SWIFT_CONNECTION_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
      socketTimeout = conf.getInt(SWIFT_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);

      writePolicy = conf.get(SWIFT_WRITE_POLICY, DEFAULT_WRITE_POLICY.name());

      throttleDelay = conf.getInt(SWIFT_THROTTLE_DELAY, DEFAULT_THROTTLE_DELAY);

      // Proxy options
      proxyHost = conf.get(SWIFT_PROXY_HOST_PROPERTY);
      proxyPort = conf.getInt(SWIFT_PROXY_PORT_PROPERTY, 8080);

      blocksizeKb = conf.getInt(SWIFT_BLOCKSIZE, DEFAULT_SWIFT_BLOCKSIZE);
      if (blocksizeKb <= 0) {
        throw new SwiftConfigurationException(
            "Invalid blocksize set in " + SWIFT_BLOCKSIZE + ": " + blocksizeKb);
      }
      partSizeKb = conf.getLong(SWIFT_PARTITION_SIZE, DEFAULT_SWIFT_PARTITION_SIZE);
      inputBufferSize =
          conf.getInt(SWIFT_INPUT_STREAM_BUFFER_SIZE, DEFAULT_SWIFT_INPUT_STREAM_BUFFER_SIZE);
      outputBufferSize =
          conf.getInt(SWIFT_OUTPUT_STREAM_BUFFER_SIZE, DEFAULT_SWIFT_INPUT_STREAM_BUFFER_SIZE);
      if (partSizeKb <= 0) {
        throw new SwiftConfigurationException(
            "Invalid partition size set in " + SWIFT_PARTITION_SIZE + ": " + partSizeKb);
      }
      partSizeBytes = partSizeKb << 10;
      bufferSizeKb = conf.getInt(SWIFT_REQUEST_SIZE, DEFAULT_SWIFT_REQUEST_SIZE);
      if (bufferSizeKb <= 0) {
        throw new SwiftConfigurationException(
            "Invalid buffer size set in " + SWIFT_REQUEST_SIZE + ": " + bufferSizeKb);
      }
    } catch (NumberFormatException e) {
      // convert exceptions raised parsing integers and longs into
      // SwiftConfigurationException instances
      throw new SwiftConfigurationException(e.toString(), e);
    }

    if (LOG.isDebugEnabled()) {
      // everything you need for diagnostics. The password is omitted.
      serviceDescription = String.format(
          "Service={%s} uri={%s}" + " tenant={%s} user={%s} region={%s}" + " publicURL={%b}"
              + " location aware={%b}" + " partition size={%d KB}, buffer size={%d KB}"
              + " block size={%d KB}" + " connect timeout={%d}, retry count={%d}"
              + " socket timeout={%d}" + " throttle delay={%d}",
          serviceProvider, stringAuthUri, tenant, username, region != null ? region : "(none)",
          usePublicUrl, locationAware, partSizeKb, bufferSizeKb, blocksizeKb, connectTimeout,
          retryCount, socketTimeout, throttleDelay);
      LOG.debug(serviceDescription);
    }
    try {
      this.authUri = new URI(stringAuthUri);
    } catch (URISyntaxException e) {
      throw new SwiftConfigurationException(
          "The " + SWIFT_AUTH_PROPERTY + " property was incorrect: " + stringAuthUri, e);
    }
  }

  public SwiftClientConfig setProperties(String container) {
    props.setProperty(SWIFT_CONTAINER_PROPERTY, container);
    return this;
  }

  /**
   * Get a mandatory configuration option.
   *
   * @param props property set
   * @param key key
   * @return value of the configuration
   * @throws SwiftConfigurationException if there was no match for the key
   */
  private static String getOption(Properties props, String key) throws SwiftConfigurationException {
    String val = props.getProperty(key);
    if (val == null) {
      throw new SwiftConfigurationException("Undefined property: " + key);
    }
    return val;
  }



  public boolean isLazySeek() {
    return isLazySeek;
  }

  public String getAuthEndpointPrefix() {
    return authEndpointPrefix;
  }

  public void setAuthEndpointPrefix(String authEndpointPrefix) {
    this.authEndpointPrefix = authEndpointPrefix;
  }

  public URI getAuthUri() {
    return authUri;
  }

  public void setAuthUri(URI authUri) {
    this.authUri = authUri;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public AuthenticationRequest getAuthRequest() {
    return authRequest;
  }

  public void setAuthRequest(AuthenticationRequest authRequest) {
    this.authRequest = authRequest;
  }

  public AuthenticationRequest getKeystoneAuthRequest() {
    return keystoneAuthRequest;
  }

  public void setKeystoneAuthRequest(AuthenticationRequest keystoneAuthRequest) {
    this.keystoneAuthRequest = keystoneAuthRequest;
  }

  public String getServiceDescription() {
    return serviceDescription;
  }

  public void setServiceDescription(String serviceDescription) {
    this.serviceDescription = serviceDescription;
  }

  public String getServiceProvider() {
    return serviceProvider;
  }

  public void setServiceProvider(String serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  public boolean isUsePublicUrl() {
    return usePublicUrl;
  }

  public void setUsePublicUrl(boolean usePublicUrl) {
    this.usePublicUrl = usePublicUrl;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public int getRetryAuth() {
    return retryAuth;
  }

  public void setRetryAuth(int retryAuth) {
    this.retryAuth = retryAuth;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public int getThrottleDelay() {
    return throttleDelay;
  }

  public void setThrottleDelay(int throttleDelay) {
    this.throttleDelay = throttleDelay;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  public boolean isLocationAware() {
    return locationAware;
  }

  public void setLocationAware(boolean locationAware) {
    this.locationAware = locationAware;
  }

  public long getPartSizeKb() {
    return partSizeKb;
  }

  public void setPartSizeKb(int partSizeKb) {
    this.partSizeKb = partSizeKb;
  }

  public long getPartSizeBytes() {
    return partSizeBytes;
  }

  public int getBlocksizeKb() {
    return blocksizeKb;
  }

  public int getBufferSizeKb() {
    return bufferSizeKb;
  }

  public void setBufferSizeKb(int bufferSizeKb) {
    this.bufferSizeKb = bufferSizeKb;
  }

  public int getMaxCoreConnections() {
    return maxCoreConnections;
  }

  public void setMaxCoreConnections(int maxCoreConnections) {
    this.maxCoreConnections = maxCoreConnections;
  }

  public int getMaxTotalConnections() {
    return maxTotalConnections;
  }

  public void setMaxTotalConnections(int maxTotalConnections) {
    this.maxTotalConnections = maxTotalConnections;
  }

  public int getInputBufferSize() {
    return inputBufferSize;
  }

  public int getOutputBufferSize() {
    return outputBufferSize;
  }

  public void setInputBufferSize(int inputBufferSize) {
    this.inputBufferSize = inputBufferSize;
  }

  public int getLruCacheSize() {
    return lruCacheSize;
  }

  public void setLruCacheSize(int lruCacheSize) {
    this.lruCacheSize = lruCacheSize;
  }

  public boolean isUseHeaderCache() {
    return useHeaderCache;
  }

  public void setUseHeaderCache(boolean useHeaderCache) {
    this.useHeaderCache = useHeaderCache;
  }

  public DurationStatsTable getDurationStats() {
    return durationStats;
  }

  public void setDurationStats(DurationStatsTable durationStats) {
    this.durationStats = durationStats;
  }

  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }

  public void setLazySeek(boolean isLazySeek) {
    this.isLazySeek = isLazySeek;
  }

  public int getMaxThreadsInPool() {
    return maxThreadsInPool;
  }

  public int getMaxThreadsForCopy() {
    return maxThreadsForCopy;
  }

  public long getCacheLiveTime() {
    return cacheLiveTime;
  }

  public int getMaxInParallelUpload() {
    return maxInParallelUpload;
  }

  /**
   * Define all write policies.
   *
   * @return write policy
   */
  public WritePolicies getWritePolicy() {
    WritePolicies cur;
    if (WritePolicies.MULTIPART_SINGLE_THREAD.name().equalsIgnoreCase(writePolicy)) {
      cur = WritePolicies.MULTIPART_SINGLE_THREAD;
    } else if (WritePolicies.MULTIPART_NO_SPLIT.name().equalsIgnoreCase(writePolicy)) {
      cur = WritePolicies.MULTIPART_NO_SPLIT;
    } else if (WritePolicies.MULTIPART_SPLIT_BLOCK.name().equalsIgnoreCase(writePolicy)) {
      cur = WritePolicies.MULTIPART_SPLIT_BLOCK;
    } else {
      cur = WritePolicies.MULTIPART_SPLIT;
    }
    return cur;
  }

}
