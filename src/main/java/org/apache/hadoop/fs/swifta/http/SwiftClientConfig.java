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
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_CONNECTIONS_IN_POOL;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_HOST_CONNECTIONS;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_MAX_TOTAL_CONNECTIONS;
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
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.USE_HEADER_CACHE;

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
import org.apache.hadoop.fs.swifta.util.DurationStatsTable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class SwiftClientConfig {


  private final static Log LOG = LogFactory.getLog(SwiftClientConfig.class);

  private final static int DEFAULT_LRU_SIZE = 100;

  /**
   * In milliseconds.
   */
  private final static long DEFAULT_EXPIRES_TIME = 10 * 60 * 1000;

  /**
   * the authentication endpoint as supplied in the configuration
   */
  private URI authUri;

  /**
   * Swift region. Some OpenStack installations has more than one region. In this case user can
   * specify the region with which Hadoop will be working
   */
  private String region;

  /**
   * tenant name
   */
  private String tenant;

  /**
   * username name
   */
  private String username;

  /**
   * user password
   */
  private String password;

  /**
   * user api key
   */
  private String apiKey;

  /**
   * The authentication request used to authenticate with Swift
   */
  private AuthenticationRequest authRequest;

  /**
   * This auth request is similar to @see authRequest, with one difference: it has another json
   * representation when authRequest one is not applicable
   */
  private AuthenticationRequest keystoneAuthRequest;

  private String serviceDescription;



  private URI filesystemURI;

  /**
   * The name of the service provider
   */
  private String serviceProvider;

  /**
   * Should the public swift endpoint be used, rather than the in-cluster one?
   */
  private boolean usePublicURL;

  /**
   * Number of times to retry a connection
   */
  private int retryCount;

  /**
   * Number of times to retry a auth
   */
  private int retryAuth;

  /**
   * Entry in the swift catalog defining the prefix used to talk to objects
   */
  private String authEndpointPrefix;

  /**
   * How long (in milliseconds) should a connection be attempted
   */
  private int connectTimeout;

  /**
   * How long (in milliseconds) should a connection be attempted
   */
  private int socketTimeout;

  /**
   * How long (in milliseconds) between bulk operations
   */
  private int throttleDelay;

  /**
   * the name of a proxy host (can be null, in which case there is no proxy)
   */
  private String proxyHost;

  /**
   * The port of a proxy. This is ignored if {@link #proxyHost} is null
   */
  private int proxyPort;

  /**
   * Flag to indicate whether or not the client should query for file location data.
   */
  private boolean locationAware;

  private int partSizeKB;
  /**
   * The blocksize of this FS
   */
  private int blocksizeKB;

  private int bufferSizeKB;

  private int maxCoreConnections;

  private int maxTotalConnections;

  private int inputBufferSize;

  private boolean isLazySeek;

  /**
   * LRU cache for HEAD request
   */
  private int lruCacheSize;

  /**
   * How long the LRU cache lives
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
   * Create a Swift Client config instance.
   *
   * @param service which cloud
   * @param container which container
   * @param conf The configuration to use to extract the binding
   * @throws SwiftConfigurationException the configuration is not valid for defining a rest client
   *         against the service
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
    usePublicURL = "true".equals(isPubProp);
    authEndpointPrefix = getOption(props, SWIFT_AUTH_ENDPOINT_PREFIX);
    maxCoreConnections = conf.getInt(SWIFT_MAX_HOST_CONNECTIONS, 0);

    maxTotalConnections = conf.getInt(SWIFT_MAX_TOTAL_CONNECTIONS, 0);
    int defaultConnections = Runtime.getRuntime().availableProcessors() * 15;
    if (defaultConnections == 0) {
      defaultConnections = 1; // Barely happen.
    }
    /**
     * Default thread pool number for http client.
     */
    if (maxCoreConnections < 1) {
      maxCoreConnections = defaultConnections;
    }
    if (maxTotalConnections < 1) {
      maxTotalConnections = defaultConnections;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Max total threads " + maxTotalConnections + "; max core threads " + maxCoreConnections);
    }
    lruCacheSize = conf.getInt(LRU_SIZE, DEFAULT_LRU_SIZE);
    this.cacheLiveTime = conf.getLong(LRU_LIVE_TIME, DEFAULT_EXPIRES_TIME);
    // Default set to false.
    isLazySeek = conf.getBoolean(SWIFT_LAZY_SEEK, Boolean.FALSE);
    useHeaderCache = conf.getBoolean(USE_HEADER_CACHE, Boolean.TRUE);

    maxThreadsInPool = conf.getInt(SWIFT_MAX_CONNECTIONS_IN_POOL, 0);

    if (maxThreadsInPool < 1) {
      maxThreadsInPool = defaultConnections;
    }

    if (apiKey == null && password == null) {
      throw new SwiftConfigurationException("Configuration for " + filesystemURI
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

      throttleDelay = conf.getInt(SWIFT_THROTTLE_DELAY, DEFAULT_THROTTLE_DELAY);

      // proxy options
      proxyHost = conf.get(SWIFT_PROXY_HOST_PROPERTY);
      proxyPort = conf.getInt(SWIFT_PROXY_PORT_PROPERTY, 8080);

      blocksizeKB = conf.getInt(SWIFT_BLOCKSIZE, DEFAULT_SWIFT_BLOCKSIZE);
      if (blocksizeKB <= 0) {
        throw new SwiftConfigurationException(
            "Invalid blocksize set in " + SWIFT_BLOCKSIZE + ": " + blocksizeKB);
      }
      partSizeKB = conf.getInt(SWIFT_PARTITION_SIZE, DEFAULT_SWIFT_PARTITION_SIZE);
      inputBufferSize =
          conf.getInt(SWIFT_INPUT_STREAM_BUFFER_SIZE, DEFAULT_SWIFT_INPUT_STREAM_BUFFER_SIZE);
      if (partSizeKB <= 0) {
        throw new SwiftConfigurationException(
            "Invalid partition size set in " + SWIFT_PARTITION_SIZE + ": " + partSizeKB);
      }

      bufferSizeKB = conf.getInt(SWIFT_REQUEST_SIZE, DEFAULT_SWIFT_REQUEST_SIZE);
      if (bufferSizeKB <= 0) {
        throw new SwiftConfigurationException(
            "Invalid buffer size set in " + SWIFT_REQUEST_SIZE + ": " + bufferSizeKB);
      }
    } catch (NumberFormatException e) {
      // convert exceptions raised parsing ints and longs into
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
          usePublicURL, locationAware, partSizeKB, bufferSizeKB, blocksizeKB, connectTimeout,
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
   * Get a mandatory configuration option
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

  public boolean isUsePublicURL() {
    return usePublicURL;
  }

  public void setUsePublicURL(boolean usePublicURL) {
    this.usePublicURL = usePublicURL;
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

  public int getPartSizeKB() {
    return partSizeKB;
  }

  public void setPartSizeKB(int partSizeKB) {
    this.partSizeKB = partSizeKB;
  }

  public int getBlocksizeKB() {
    return blocksizeKB;
  }

  public void setBlocksizeKB(int blocksizeKB) {
    this.blocksizeKB = blocksizeKB;
  }

  public int getBufferSizeKB() {
    return bufferSizeKB;
  }

  public void setBufferSizeKB(int bufferSizeKB) {
    this.bufferSizeKB = bufferSizeKB;
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

  public long getCacheLiveTime() {
    return cacheLiveTime;
  }

}
