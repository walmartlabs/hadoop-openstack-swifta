/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.fs.swifta.http;

import static org.apache.commons.httpclient.HttpStatus.SC_ACCEPTED;
import static org.apache.commons.httpclient.HttpStatus.SC_BAD_REQUEST;
import static org.apache.commons.httpclient.HttpStatus.SC_CREATED;
import static org.apache.commons.httpclient.HttpStatus.SC_FORBIDDEN;
import static org.apache.commons.httpclient.HttpStatus.SC_MULTI_STATUS;
import static org.apache.commons.httpclient.HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION;
import static org.apache.commons.httpclient.HttpStatus.SC_NOT_FOUND;
import static org.apache.commons.httpclient.HttpStatus.SC_NO_CONTENT;
import static org.apache.commons.httpclient.HttpStatus.SC_OK;
import static org.apache.commons.httpclient.HttpStatus.SC_PARTIAL_CONTENT;
import static org.apache.commons.httpclient.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.commons.httpclient.HttpStatus.SC_RESET_CONTENT;
import static org.apache.commons.httpclient.HttpStatus.SC_UNAUTHORIZED;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_AUTH_KEY;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_CONTENT_LENGTH;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_CONTENT_RANGE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_DESTINATION;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_RANGE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.HEADER_USER_AGENT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SERVICE_CATALOG_CLOUD_FILES;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SERVICE_CATALOG_OBJECT_STORE;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SERVICE_CATALOG_SWIFT;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_RANGE_HEADER_FORMAT_PATTERN;
import static org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.SWIFT_USER_AGENT;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swifta.auth.AuthenticationRequest;
import org.apache.hadoop.fs.swifta.auth.AuthenticationRequestWrapper;
import org.apache.hadoop.fs.swifta.auth.AuthenticationResponse;
import org.apache.hadoop.fs.swifta.auth.AuthenticationWrapper;
import org.apache.hadoop.fs.swifta.auth.entities.AccessToken;
import org.apache.hadoop.fs.swifta.auth.entities.Catalog;
import org.apache.hadoop.fs.swifta.auth.entities.Endpoint;
import org.apache.hadoop.fs.swifta.exceptions.SwiftAuthenticationFailedException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftBadRequestException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftInternalStateException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftThrottledRequestException;
import org.apache.hadoop.fs.swifta.metrics.MetricsFactory;
import org.apache.hadoop.fs.swifta.snative.LRUCache;
import org.apache.hadoop.fs.swifta.util.Duration;
import org.apache.hadoop.fs.swifta.util.DurationStats;
import org.apache.hadoop.fs.swifta.util.DurationStatsTable;
import org.apache.hadoop.fs.swifta.util.JsonUtil;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.http.conn.params.ConnRoutePNames;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * This implements the client-side of the Swift REST API.
 * <p>
 * The core actions put, get and query data in the Swift object store, after authenticationg the client.
 * </p>
 * <b>Logging:</b>
 *
 * Logging at DEBUG level displays detail about the actions of this client, including HTTP requests and responses -excluding authentication details.
 */
public final class SwiftRestClient {

  private static final Log LOG = LogFactory.getLog(SwiftRestClient.class);
  private static final int TOLERANT_TIME = 5000;


  /**
   * Header that says "use newest version" -ensures that the query doesn't pick up older versions served by an eventually consistent filesystem (except in the special case of a network partition, at
   * which point no guarantees about consistency can be made.
   */
  public static final Header NEWEST = new Header(SwiftProtocolConstants.X_NEWEST, "true");

  private static final MetricsFactory metric = MetricsFactory.getMetricsFactory(SwiftRestClient.class);

  private boolean useKeystoneAuthentication = false;

  private final SwiftClientConfig clientConfig;

  /**
   * Endpoint for swift operations, obtained after authentication
   */
  private URI endpointURI;


  /**
   * The container this client is working with
   */
  private String container;

  /**
   * Access token (Secret)
   */
  private AccessToken token;

  /**
   * URI under which objects can be found. This is set when the user is authenticated -the URI is returned in the body of the success response.
   */
  private URI objectLocationURI;


  /**
   * Cache for file size
   */
  private LRUCache<Long> fileLen;

  private final DurationStatsTable durationStats = new DurationStatsTable();

  /**
   * objects query endpoint. This is synchronized to handle a simultaneous update of all auth data in one go.
   */
  private synchronized URI getEndpointURI() {
    return endpointURI;
  }

  /**
   * object location endpoint
   */
  private synchronized URI getObjectLocationURI() {
    return objectLocationURI;
  }

  /**
   * Setter of authentication and endpoint details. Being synchronized guarantees that all three fields are set up together. It is up to the reader to read all three fields in their own synchronized
   * block to be sure that they are all consistent.
   *
   * @param endpoint endpoint URI
   * @param objectLocation object location URI
   * @param authToken auth token
   */
  private void setAuthDetails(URI endpoint, URI objectLocation, AccessToken authToken) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("setAuth: endpoint=%s; objectURI=%s; token=%s", endpoint, objectLocation, authToken));
    }
    synchronized (this) {
      endpointURI = endpoint;
      objectLocationURI = objectLocation;
      token = authToken;
    }
  }

  /**
   * token for Swift communication
   */
  private synchronized AccessToken getToken() {
    return token;
  }

  /**
   * Base class for all Swift REST operations
   *
   * @param <M> method
   * @param <R> result
   */
  private static abstract class HttpMethodProcessor<M extends HttpMethod, R> {
    public final M createMethod(String uri) throws IOException {
      final M method = doCreateMethod(uri);
      setup(method);
      return method;
    }

    /**
     * Override it to return some result after method is executed.
     */
    public abstract R extractResult(M method) throws IOException;

    /**
     * Factory method to create a REST method against the given URI
     *
     * @param uri target
     * @return method to invoke
     */
    protected abstract M doCreateMethod(String uri);

    /**
     * Override port to set up the method before it is executed.
     */
    protected void setup(M method) throws IOException {}

    /**
     * Override point: what are the status codes that this operation supports
     *
     * @return an array with the permitted status code(s)
     */
    protected int[] getAllowedStatusCodes() {
      return new int[] {SC_OK, SC_CREATED, SC_ACCEPTED, SC_NO_CONTENT, SC_PARTIAL_CONTENT,};
    }
  }

  private static abstract class GetStreamMethodProcessor<R> extends HttpMethodProcessor<GetMethod, R> {

    @Override
    protected final GetMethod doCreateMethod(String uri) {
      return new GetMethod(uri);
    }

  }
  private static abstract class GetMethodProcessor<R> extends HttpMethodProcessor<GetMethod, R> {
    @Override
    protected final GetMethod doCreateMethod(String uri) {
      return new GetMethod(uri);
    }
  }

  /**
   * There's a special type for auth messages, so that low-level message handlers can react to auth failures differently from everything else.
   */
  private static class AuthPostMethod extends PostMethod {


    private AuthPostMethod(String uri) {
      super(uri);
    }
  }

  /**
   * Generate an auth message
   * 
   * @param <R> response
   */
  private static abstract class AuthMethodProcessor<R> extends HttpMethodProcessor<AuthPostMethod, R> {
    @Override
    protected final AuthPostMethod doCreateMethod(String uri) {
      return new AuthPostMethod(uri);
    }
  }

  private static abstract class PutMethodProcessor<R> extends HttpMethodProcessor<PutMethod, R> {
    @Override
    protected final PutMethod doCreateMethod(String uri) {
      return new PutMethod(uri);
    }

    /**
     * Override point: what are the status codes that this operation supports
     *
     * @return the list of status codes to accept
     */
    @Override
    protected int[] getAllowedStatusCodes() {
      return new int[] {SC_OK, SC_CREATED, SC_NO_CONTENT, SC_ACCEPTED,};
    }
  }

  /**
   * Create operation
   *
   * @param <R>
   */
  private static abstract class CopyMethodProcessor<R> extends HttpMethodProcessor<CopyMethod, R> {
    @Override
    protected final CopyMethod doCreateMethod(String uri) {
      return new CopyMethod(uri);
    }

    /**
     * The only allowed status code is 201:created
     * 
     * @return an array with the permitted status code(s)
     */
    @Override
    protected int[] getAllowedStatusCodes() {
      return new int[] {SC_CREATED};
    }
  }

  /**
   * Delete operation
   *
   * @param <R>
   */
  private static abstract class DeleteMethodProcessor<R> extends HttpMethodProcessor<DeleteMethod, R> {
    @Override
    protected final DeleteMethod doCreateMethod(String uri) {
      return new DeleteMethod(uri);
    }

    @Override
    protected int[] getAllowedStatusCodes() {
      return new int[] {SC_OK, SC_ACCEPTED, SC_NO_CONTENT, SC_NOT_FOUND};
    }
  }

  private static abstract class HeadMethodProcessor<R> extends HttpMethodProcessor<HeadMethod, R> {
    @Override
    protected final HeadMethod doCreateMethod(String uri) {
      return new HeadMethod(uri);
    }
  }

  /**
   * Create a Swift Rest Client instance.
   *
   * @param filesystemURI filesystem URI
   * @param conf The configuration to use to extract the binding
   * @throws SwiftConfigurationException the configuration is not valid for defining a rest client against the service
   */
  private SwiftRestClient(URI filesystemURI, Configuration conf) throws SwiftConfigurationException {

    String host = filesystemURI.getHost();
    if (host == null || host.isEmpty()) {
      // expect shortnames -> conf names
      throw new SwiftConfigurationException(String.format(RestClientBindings.E_INVALID_NAME, host));
    }
    this.setContainer(RestClientBindings.extractContainerName(host));
    String service = RestClientBindings.extractServiceName(host);
    clientConfig = SwiftClientConfigFactory.getInstance(service, conf);
    this.setContainer(RestClientBindings.extractContainerName(host));
    metric.increase(this);
    metric.report();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Filesystem " + filesystemURI + " is using configuration keys " + service);
    }
  }

  public SwiftClientConfig getClientConfig() {
    return clientConfig;
  }

  private HttpClient initHttpClient(SwiftClientConfig clientConfig) {
    return new HttpClient(HttpClientManager.getHttpManager(clientConfig));
  }

  /**
   * Make an HTTP GET request to Swift to get a range of data in the object.
   *
   * @param url url to object
   * @param offset offset from file beginning
   * @param length file length
   * @return The input stream -which must be closed afterwards.
   * @throws IOException Problems
   * @throws SwiftException swift specific error
   * @throws FileNotFoundException path is not there
   */
  public HttpBodyContent getData(URI url, long offset, long length) throws IOException {
    if (offset < 0) {
      throw new SwiftException("Invalid offset: " + offset + " in getDataAsInputStream( url=" + url + ", offset=" + offset + ", length =" + length + ")");
    }
    if (length <= 0) {
      throw new SwiftException("Invalid length: " + length + " in getDataAsInputStream( url=" + url + ", offset=" + offset + ", length =" + length + ")");
    }
    final String range = String.format(SWIFT_RANGE_HEADER_FORMAT_PATTERN, offset, length - 1);
    if (LOG.isDebugEnabled()) {
      String msg = String.format("Get data %s, from %s", range, url);
      LOG.debug(msg);
    }

    // preRemoteCommand("getData");
    return getData(url, new Header(HEADER_RANGE, range), SwiftRestClient.NEWEST);
  }

  /**
   * Make an HTTP GET request to Swift to get a range of data in the object.
   *
   * @param path path to object
   * @param offset offset from file beginning
   * @param length file length
   * @return The input stream -which must be closed afterwards.
   * @throws IOException Problems
   * @throws SwiftException swift specific error
   * @throws FileNotFoundException path is not there
   */
  public HttpBodyContent getData(SwiftObjectPath path, long offset, long length) throws IOException {
    preRemoteCommand("getData");
    return getData(pathToURI(path), offset, length);
  }


  /**
   * Returns object length
   *
   * @param uri file URI
   * @return object length
   * @throws SwiftException on swift-related issues
   * @throws IOException on network/IO problems
   */
  public long getContentLength(final URI uri) throws IOException {
    preRemoteCommand("getContentLength");
    return perform("getContentLength", uri, new HeadMethodProcessor<Long>() {
      @Override
      public Long extractResult(HeadMethod method) throws IOException {
        long l = method.getResponseContentLength();
        if (l > 0) {
          fileLen.set(uri.getPath(), l);
        }
        return l;
      }

      @Override
      protected void setup(HeadMethod method) throws IOException {
        super.setup(method);
        method.addRequestHeader(NEWEST);
      }
    });
  }

  /**
   * Get the length of the remote object
   * 
   * @param path object to probe
   * @return the content length
   * @throws IOException on any failure
   */
  public long getContentLength(SwiftObjectPath path) throws IOException {
    return getContentLength(pathToURI(path));
  }

  /**
   * Get the path contents as an input stream. <b>Warning:</b> this input stream must be closed to avoid keeping Http connections open.
   *
   * @param path path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if there is nothing at the path
   */
  public HttpBodyContent getData(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("getData");
    return getData(pathToURI(path), requestHeaders);
  }

  /**
   * Get the path contents as an input stream. <b>Warning:</b> this input stream must be closed to avoid keeping Http connections open.
   *
   * @param url path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if there is nothing at the path
   */
  public HttpBodyContent getData(URI url, final Header... requestHeaders) throws IOException {
    preRemoteCommand("getData");
    return doGet(url, requestHeaders);
  }

  /**
   * Returns object location as byte[]
   *
   * @param path path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   */
  public byte[] getObjectLocation(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    if (!clientConfig.isLocationAware()) {
      // if the filesystem is not location aware, do not ask for this information
      return null;
    }
    preRemoteCommand("getObjectLocation");
    try {
      return perform("getObjectLocation", pathToObjectLocation(path), new GetMethodProcessor<byte[]>() {
        @Override
        protected int[] getAllowedStatusCodes() {
          return new int[] {SC_OK, SC_FORBIDDEN, SC_NO_CONTENT};
        }

        @Override
        public byte[] extractResult(GetMethod method) throws IOException {

          // TODO: remove SC_NO_CONTENT if it depends on Swift versions
          if (method.getStatusCode() == SC_NOT_FOUND || method.getStatusCode() == SC_FORBIDDEN || method.getStatusCode() == SC_NO_CONTENT || method.getResponseBodyAsStream() == null) {
            return null;
          }
          final InputStream responseBodyAsStream = method.getResponseBodyAsStream();
          final byte[] locationData = new byte[1024];
          return responseBodyAsStream.read(locationData) > 0 ? locationData : null;
        }

        @Override
        protected void setup(GetMethod method) throws SwiftInternalStateException {
          setHeaders(method, requestHeaders);
        }
      });
    } catch (IOException e) {
      LOG.warn("Failed to get the location of " + path + ": " + e, e);
      return null;
    }
  }

  /**
   * Create the URI needed to query the location of an object
   * 
   * @param path object path to retrieve information about
   * @return the URI for the location operation
   * @throws SwiftException if the URI could not be constructed
   */
  private URI pathToObjectLocation(SwiftObjectPath path) throws SwiftException {
    URI uri;
    String dataLocationURI = getObjectLocationURI().toString();
    try {
      if (path.toString().startsWith("/")) {
        dataLocationURI = dataLocationURI.concat(path.toUriPath());
      } else {
        dataLocationURI = dataLocationURI.concat("/").concat(path.toUriPath());
      }

      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException(e);
    }
    return uri;
  }

  /**
   * Find objects under a prefix
   *
   * @param path path prefix
   * @param requestHeaders optional request headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if nothing is at the end of the URI -that is, the directory is empty
   */
  @SuppressWarnings("unused")
  private byte[] findObjectsByPrefix(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("findObjectsByPrefix");
    URI uri;
    String dataLocationURI = getEndpointURI().toString();
    try {
      String object = path.getObject();
      if (object.startsWith("/")) {
        object = object.substring(1);
      }
      object = SwiftUtils.encodeUrl(object);
      dataLocationURI = dataLocationURI.concat("/").concat(path.getContainer()).concat("/?prefix=").concat(object);
      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException("Bad URI: " + dataLocationURI, e);
    }

    return perform("findObjectsByPrefix", uri, new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          // no result
          throw new FileNotFoundException("Not found " + method.getURI());
        }
        return method.getResponseBody();
      }

      @Override
      protected int[] getAllowedStatusCodes() {
        return new int[] {SC_OK, SC_NOT_FOUND};
      }

      @Override
      protected void setup(GetMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Find objects in a directory
   *
   * @param path path prefix
   * @param requestHeaders optional request headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if nothing is at the end of the URI -that is, the directory is empty
   */
  public byte[] listDeepObjectsInDirectory(SwiftObjectPath path, boolean listDeep, String marker, final Header... requestHeaders) throws IOException {
    preRemoteCommand("listDeepObjectsInDirectory");

    String endpoint = getEndpointURI().toString();
    StringBuilder dataLocationURI = new StringBuilder();
    dataLocationURI.append(endpoint);
    String object = path.getObject();
    if (object.startsWith("/")) {
      object = object.substring(1);
    }
    if (!object.endsWith("/")) {
      object = object.concat("/");
    }

    if (object.equals("/")) {
      object = "";
    }

    dataLocationURI = dataLocationURI.append("/").append(path.getContainer()).append("/?prefix=").append(object).append("&format=json");

    if (marker != null) {
      if (marker.startsWith("/")) {
        marker = marker.substring(1);
      }
      dataLocationURI.append("&marker=").append(marker);
    }

    // in listing deep set param to false
    if (listDeep == false) {
      dataLocationURI.append("&delimiter=/");
    }
    return findObjects(dataLocationURI.toString(), requestHeaders);
  }

  /**
   * Find objects in a location
   * 
   * @param location URI
   * @param requestHeaders optional request headers
   * @return the body of te response
   * @throws IOException IO problems
   */
  private byte[] findObjects(String location, final Header[] requestHeaders) throws IOException {
    URI uri;
    preRemoteCommand("findObjects");
    try {
      uri = new URI(location);
    } catch (URISyntaxException e) {
      throw new SwiftException("Bad URI: " + location, e);
    }

    return perform("findObjects", uri, new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          // no result
          throw new FileNotFoundException("Not found " + method.getURI());
        }
        return method.getResponseBody();
      }

      @Override
      protected int[] getAllowedStatusCodes() {
        return new int[] {SC_OK, SC_NOT_FOUND};
      }

      @Override
      protected void setup(GetMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Copy an object. This is done by sending a COPY method to the filesystem which is required to handle this WebDAV-level extension to the base HTTP operations.
   *
   * @param src source path
   * @param dst destination path
   * @param headers any headers
   * @return true if the status code was considered successful
   * @throws IOException on IO Faults
   */
  public boolean copyObject(SwiftObjectPath src, final SwiftObjectPath dst, final Header... headers) throws IOException {

    preRemoteCommand("copyObject");

    return perform("copy", pathToURI(src), new CopyMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(CopyMethod method) throws IOException {
        return method.getStatusCode() != SC_NOT_FOUND;
      }

      @Override
      protected void setup(CopyMethod method) throws SwiftInternalStateException {
        setHeaders(method, headers);
        method.addRequestHeader(HEADER_DESTINATION, dst.toUriPath());
      }
    });
  }

  /**
   * Uploads file as Input Stream to Swift. The data stream will be closed after the request.
   *
   * @param path path to Swift
   * @param data object data
   * @param length length of data
   * @param requestHeaders http headers
   * @throws IOException on IO Faults
   */
  public void upload(SwiftObjectPath path, final InputStream data, final long length, final Header... requestHeaders) throws IOException {
    preRemoteCommand("upload");

    try {
      perform("upload", pathToURI(path), new PutMethodProcessor<byte[]>() {
        @Override
        public byte[] extractResult(PutMethod method) throws IOException {
          return method.getResponseBody();
        }

        @Override
        protected void setup(PutMethod method) throws SwiftInternalStateException {
          method.setRequestEntity(new InputStreamRequestEntity(data, length));
          setHeaders(method, requestHeaders);
        }
      });
    } finally {
      data.close();
    }

  }


  /**
   * Deletes object from swift. The result is true if this operation did the deletion.
   *
   * @param path path to file
   * @param requestHeaders http headers
   * @throws IOException on IO Faults
   */
  public boolean delete(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("delete");

    return perform("", pathToURI(path), new DeleteMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(DeleteMethod method) throws IOException {
        return method.getStatusCode() == SC_NO_CONTENT;
      }

      @Override
      protected void setup(DeleteMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Issue a head request
   * 
   * @param reason reason -used in logs
   * @param path path to query
   * @param requestHeaders request header
   * @return the response headers. This may be an empty list
   * @throws IOException IO problems
   * @throws FileNotFoundException if there is nothing at the end
   */
  public Header[] headRequest(String reason, SwiftObjectPath path, final Header... requestHeaders) throws IOException {

    preRemoteCommand("headRequest: " + reason);
    return perform(reason, pathToURI(path), new HeadMethodProcessor<Header[]>() {
      @Override
      public Header[] extractResult(HeadMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          throw new FileNotFoundException("Not Found " + method.getURI());
        }

        return method.getResponseHeaders();
      }

      @Override
      protected void setup(HeadMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Issue a put request
   * 
   * @param path path
   * @param requestHeaders optional headers
   * @return the HTTP response
   * @throws IOException any problem
   */
  public int putRequest(SwiftObjectPath path, final Header... requestHeaders) throws IOException {

    preRemoteCommand("putRequest");
    return perform(pathToURI(path), new PutMethodProcessor<Integer>() {

      @Override
      public Integer extractResult(PutMethod method) throws IOException {
        return method.getStatusCode();
      }

      @Override
      protected void setup(PutMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Authenticate to Openstack Keystone As well as returning the access token, the member fields {@link #token}, {@link #endpointURI} and {@link #objectLocationURI} are set up for re-use.
   * <p/>
   * This method is re-entrant -if more than one thread attempts to authenticate neither will block -but the field values with have those of the last caller.
   * <p/>
   *
   * @return authenticated access token
   */
  public AccessToken authenticate() throws IOException {
    final AuthenticationRequest authenticationRequest;
    if (useKeystoneAuthentication) {
      authenticationRequest = this.clientConfig.getKeystoneAuthRequest();
    } else {
      authenticationRequest = this.clientConfig.getAuthRequest();
    }

    LOG.debug("started authentication");
    return perform("authentication", this.clientConfig.getAuthUri(), new AuthenticationPost(authenticationRequest));
  }

  private class AuthenticationPost extends AuthMethodProcessor<AccessToken> {
    final AuthenticationRequest authenticationRequest;

    private AuthenticationPost(AuthenticationRequest authenticationRequest) {
      this.authenticationRequest = authenticationRequest;
    }

    @Override
    protected void setup(AuthPostMethod method) throws IOException {

      method.setRequestEntity(getAuthenticationRequst(authenticationRequest));
    }

    /**
     * specification says any of the 2xxs are OK, so list all the standard ones
     * 
     * @return a set of 2XX status codes.
     */
    @Override
    protected int[] getAllowedStatusCodes() {
      return new int[] {SC_OK, SC_BAD_REQUEST, SC_CREATED, SC_ACCEPTED, SC_NON_AUTHORITATIVE_INFORMATION, SC_NO_CONTENT, SC_RESET_CONTENT, SC_PARTIAL_CONTENT, SC_MULTI_STATUS, SC_UNAUTHORIZED // if
                                                                                                                                                                                                // request
                                                                                                                                                                                                // unauthorized,
                                                                                                                                                                                                // try
                                                                                                                                                                                                // another
                                                                                                                                                                                                // method
      };
    }

    @SuppressWarnings("unused")
    @Override
    public AccessToken extractResult(AuthPostMethod method) throws IOException {
      // initial check for failure codes leading to authentication failures
      if (method.getStatusCode() == SC_BAD_REQUEST) {
        throw new SwiftAuthenticationFailedException(authenticationRequest.toString(), "POST", clientConfig.getAuthUri(), method);
      }

      final AuthenticationResponse access = JsonUtil.toObject(method.getResponseBodyAsString(), AuthenticationWrapper.class).getAccess();
      final List<Catalog> serviceCatalog = access.getServiceCatalog();
      // locate the specific service catalog that defines Swift; variations
      // in the name of this add complexity to the search
      boolean catalogMatch = false;
      StringBuilder catList = new StringBuilder();
      StringBuilder regionList = new StringBuilder();

      // these fields are all set together at the end of the operation
      URI endpointURI = null;
      URI objectLocation;
      Endpoint swiftEndpoint = null;
      AccessToken accessToken;

      for (Catalog catalog : serviceCatalog) {
        String name = catalog.getName();
        String type = catalog.getType();
        String descr = String.format("[%s: %s]; ", name, type);
        catList.append(descr);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Catalog entry " + descr);
        }
        if (name.equals(SERVICE_CATALOG_SWIFT) || name.equals(SERVICE_CATALOG_CLOUD_FILES) || type.equals(SERVICE_CATALOG_OBJECT_STORE)) {
          // swift is found
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found swift catalog as " + name + " => " + type);
          }
          // now go through the endpoints
          for (Endpoint endpoint : catalog.getEndpoints()) {
            String endpointRegion = endpoint.getRegion();
            URI publicURL = endpoint.getPublicURL();
            URI internalURL = endpoint.getInternalURL();
            descr = String.format("[%s => %s / %s]; ", endpointRegion, publicURL, internalURL);
            regionList.append(descr);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Endpoint " + descr);
            }
            if (clientConfig.getRegion() == null || endpointRegion.equals(clientConfig.getRegion())) {
              endpointURI = clientConfig.isUsePublicURL() ? publicURL : internalURL;
              swiftEndpoint = endpoint;
              break;
            }
          }
        }
      }
      if (endpointURI == null) {
        String message = "Could not find swift service from auth URL " + clientConfig.getAuthUri() + " and region '" + clientConfig.getRegion() + "'. " + "Categories: " + catList
            + ((regionList.length() > 0) ? ("regions: " + regionList) : "No regions");
        throw new SwiftInvalidResponseException(message, SC_OK, "authenticating", clientConfig.getAuthUri());

      }


      accessToken = access.getToken();
      String path = clientConfig.getAuthEndpointPrefix() + accessToken.getTenant().getId();
      String host = endpointURI.getHost();
      try {
        objectLocation = new URI(endpointURI.getScheme(), null, host, endpointURI.getPort(), path, null, null);
      } catch (URISyntaxException e) {
        throw new SwiftException("object endpoint URI is incorrect: " + endpointURI + " + " + path, e);
      }
      setAuthDetails(endpointURI, objectLocation, accessToken);

      if (LOG.isDebugEnabled()) {
        LOG.debug("authenticated against " + endpointURI);
      }
      // createDefaultContainer();
      return accessToken;
    }
  }

  private StringRequestEntity getAuthenticationRequst(AuthenticationRequest authenticationRequest) throws IOException {
    final String data = JsonUtil.toJson(new AuthenticationRequestWrapper(authenticationRequest));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Authenticating with " + authenticationRequest);
    }
    return toJsonEntity(data);
  }

  /**
   * Create a container -if it already exists, do nothing
   *
   * @param containerName the container name
   * @throws IOException IO problems
   * @throws SwiftBadRequestException invalid container name
   * @throws SwiftInvalidResponseException error from the server
   */
  public void createContainer(String containerName) throws IOException {
    SwiftObjectPath objectPath = new SwiftObjectPath(containerName, "");
    try {
      // see if the container is there
      headRequest("createContainer", objectPath, NEWEST);
    } catch (FileNotFoundException ex) {
      int status = 0;
      try {
        status = putRequest(objectPath);
      } catch (FileNotFoundException e) {
        // triggered by a very bad container name.
        // re-insert the 404 result into the status
        status = SC_NOT_FOUND;
      }
      if (status == SC_BAD_REQUEST) {
        throw new SwiftBadRequestException("Bad request -authentication failure or bad container name?", status, "PUT", null);
      }
      if (!isStatusCodeExpected(status, SC_OK, SC_CREATED, SC_ACCEPTED, SC_NO_CONTENT)) {
        throw new SwiftInvalidResponseException("Couldn't create container " + containerName + " for storing data in Swift." + " Try to create container " + containerName + " manually ", status,
            "PUT", null);
      }
    }
  }

  /**
   * Check if the container exists or not
   *
   * @param containerName the container name
   * @return true if the container exists
   */
  public boolean doesExistContainer(String containerName) throws IOException {
    SwiftObjectPath objectPath = new SwiftObjectPath(containerName, "");
    try {
      // see if the container is there
      headRequest("createContainer", objectPath, NEWEST);
      return true;
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  /**
   * Trigger an initial auth operation if some of the needed fields are missing
   *
   * @throws IOException on problems
   */
  private void authIfNeeded() throws IOException {
    if (getEndpointURI() == null) {
      authenticate();
    }
  }

  /**
   * Pre-execution actions to be performed by methods. Currently this
   * <ul>
   * <li>Logs the operation at TRACE</li>
   * <li>Authenticates the client -if needed</li>
   * </ul>
   * 
   * @throws IOException
   */
  private void preRemoteCommand(String operation) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Executing " + operation);
    }
    authIfNeeded();
  }


  /**
   * Performs the HTTP request, validates the response code and returns the received data. HTTP Status codes are converted into exceptions.
   *
   * @param uri URI to source
   * @param processor HttpMethodProcessor
   * @param <M> method
   * @param <R> result type
   * @return result of HTTP request
   * @throws IOException IO problems
   * @throws SwiftBadRequestException the status code indicated "Bad request"
   * @throws SwiftInvalidResponseException the status code is out of range for the action (excluding 404 responses)
   * @throws SwiftInternalStateException the internal state of this client is invalid
   * @throws FileNotFoundException a 404 response was returned
   */
  private <M extends HttpMethod, R> R perform(URI uri, HttpMethodProcessor<M, R> processor)
      throws IOException, SwiftBadRequestException, SwiftInternalStateException, SwiftInvalidResponseException, FileNotFoundException {
    return perform("", uri, processor);
  }

  /**
   * Performs the HTTP request, validates the response code and returns the received data. HTTP Status codes are converted into exceptions.
   * 
   * @param reason why is this operation taking place. Used for statistics
   * @param uri URI to source
   * @param processor HttpMethodProcessor
   * @param <M> method
   * @param <R> result type
   * @return result of HTTP request
   * @throws IOException IO problems
   * @throws SwiftBadRequestException the status code indicated "Bad request"
   * @throws SwiftInvalidResponseException the status code is out of range for the action (excluding 404 responses)
   * @throws SwiftInternalStateException the internal state of this client is invalid
   * @throws FileNotFoundException a 404 response was returned
   */
  private <M extends HttpMethod, R> R perform(String reason, URI uri, HttpMethodProcessor<M, R> processor)
      throws IOException, SwiftBadRequestException, SwiftInternalStateException, SwiftInvalidResponseException, FileNotFoundException {
    checkNotNull(uri);
    checkNotNull(processor);

    final M method = processor.createMethod(uri.toString());

    // retry policy
    HttpMethodParams methodParams = method.getParams();
    methodParams.setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(clientConfig.getRetryCount(), false));
    methodParams.setIntParameter(HttpConnectionParams.CONNECTION_TIMEOUT, clientConfig.getConnectTimeout());
    methodParams.setSoTimeout(clientConfig.getSocketTimeout());
    method.addRequestHeader(HEADER_USER_AGENT, SWIFT_USER_AGENT);
    Duration duration = new Duration();
    boolean success = false;
    try {
      int statusCode = 0;
      try {
        long times = System.currentTimeMillis();
        statusCode = exec(method);
        if ((System.currentTimeMillis() - times) > TOLERANT_TIME) {
          LOG.warn("Servers take more than " + TOLERANT_TIME / 1000 + " seconds to response." + uri.toString());
        }
      } catch (IOException e) {
        // rethrow with extra diagnostics and wiki links
        throw ExceptionDiags.wrapException(uri.toString(), method.getName(), e);
      }

      // look at the response and see if it was valid or not.
      // Valid is more than a simple 200; even 404 "not found" is considered
      // valid -which it is for many methods.

      // validate the allowed status code for this operation
      int[] allowedStatusCodes = processor.getAllowedStatusCodes();
      boolean validResponse = isStatusCodeExpected(statusCode, allowedStatusCodes);

      if (!validResponse) {
        IOException ioe = buildException(uri, method, statusCode);
        throw ioe;
      }

      R r = processor.extractResult(method);
      success = true;
      return r;
    } catch (IOException e) {
      // release the connection -always
      if (processor instanceof GetStreamMethodProcessor) {
        method.releaseConnection();
      }
      throw e;
    } finally {
      // Why not always release connection? It might cause issue for streams.
      // method.releaseConnection();
      if (processor instanceof GetStreamMethodProcessor) {
        // Do not close connection here.
      } else {
        // LOG.info("Close connection " + processor.getClass());
        method.releaseConnection();
      }
      duration.finished();
      durationStats.add(method.getName() + " " + reason, duration, success);
    }
  }

  /**
   * Build an exception from a failed operation. This can include generating specific exceptions (e.g. FileNotFound), as well as the default {@link SwiftInvalidResponseException}.
   *
   * @param uri URI for operation
   * @param method operation that failed
   * @param statusCode status code
   * @param <M> method type
   * @return an exception to throw
   */
  private <M extends HttpMethod> IOException buildException(URI uri, M method, int statusCode) {
    IOException fault;

    // log the failure @debug level
    String errorMessage = String.format("Method %s on %s failed, status code: %d," + " status line: %s", method.getName(), uri, statusCode, method.getStatusLine());
    if (LOG.isDebugEnabled()) {
      LOG.debug(errorMessage);
    }
    // send the command
    switch (statusCode) {
      case SC_NOT_FOUND:
        fault = new FileNotFoundException("Operation " + method.getName() + " on " + uri);
        break;

      case SC_BAD_REQUEST:
        // bad HTTP request
        fault = new SwiftBadRequestException("Bad request against " + uri, method.getName(), uri, method);
        break;

      case SC_REQUESTED_RANGE_NOT_SATISFIABLE:
        // out of range
        StringBuilder errorText = new StringBuilder(method.getStatusText());
        // get the requested length
        Header requestContentLen = method.getRequestHeader(HEADER_CONTENT_LENGTH);
        if (requestContentLen != null) {
          errorText.append(" requested ").append(requestContentLen.getValue());
        }
        // and the result
        Header availableContentRange = method.getResponseHeader(HEADER_CONTENT_RANGE);
        if (requestContentLen != null) {
          errorText.append(" available ").append(availableContentRange.getValue());
        }
        fault = new EOFException(errorText.toString());
        break;

      case SC_UNAUTHORIZED:
        // auth failure; should only happen on the second attempt
        fault = new SwiftAuthenticationFailedException("Operation not authorized- current access token =" + getToken(), method.getName(), uri, method);
        break;

      case SwiftProtocolConstants.SC_TOO_MANY_REQUESTS_429:
      case SwiftProtocolConstants.SC_THROTTLED_498:
        // response code that may mean the client is being throttled
        fault = new SwiftThrottledRequestException("Client is being throttled: too many requests", method.getName(), uri, method);
        break;

      default:
        // return a generic invalid HTTP response
        fault = new SwiftInvalidResponseException(errorMessage, method.getName(), uri, method);
    }

    return fault;
  }

  /**
   * Exec a GET request and return the input stream of the response
   *
   * @param uri URI to GET
   * @param requestHeaders request headers
   * @return the input stream. This must be closed to avoid log errors
   * @throws IOException
   */
  private HttpBodyContent doGet(final URI uri, final Header... requestHeaders) throws IOException {
    return perform("", uri, new GetStreamMethodProcessor<HttpBodyContent>() {

      @Override
      protected void setup(GetMethod method) throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }

      @Override
      public HttpBodyContent extractResult(GetMethod method) throws IOException {
        return new HttpBodyContent(new HttpInputStreamWithRelease(uri, method, clientConfig.getInputBufferSize()), method.getResponseContentLength());
      }
    });
  }

  /**
   * Create an instance against a specific FS URI,
   *
   * @param filesystemURI filesystem to bond to
   * @param config source of configuration data
   * @return REST client instance
   * @throws IOException on instantiation problems
   */
  public static SwiftRestClient getInstance(URI filesystemURI, Configuration config) throws IOException {
    return new SwiftRestClient(filesystemURI, config);
  }


  /**
   * Convert the (JSON) data to a string request as UTF-8
   *
   * @param data data
   * @return the data
   * @throws SwiftException if for some very unexpected reason it's impossible to convert the data to UTF-8.
   */
  private static StringRequestEntity toJsonEntity(String data) throws SwiftException {
    StringRequestEntity entity;
    try {
      entity = new StringRequestEntity(data, "application/json", "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new SwiftException("Could not encode data as UTF-8", e);
    }
    return entity;
  }

  /**
   * Converts Swift path to URI to make request. This is public for unit testing
   *
   * @param path path to object
   * @param endpointURI damain url e.g. http://domain.com
   * @return valid URI for object
   */
  public static URI pathToURI(SwiftObjectPath path, URI endpointURI) throws SwiftException {
    checkNotNull(endpointURI, "Null Endpoint -client is not authenticated");

    String dataLocationURI = endpointURI.toString();
    try {

      dataLocationURI = SwiftUtils.joinPaths(dataLocationURI, SwiftUtils.encodeUrl(path.toUriPath()));
      return new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException("Failed to create URI from " + dataLocationURI, e);
    }
  }

  /**
   * Convert a swift path to a URI relative to the current endpoint.
   *
   * @param path path
   * @return an path off the current endpoint URI.
   * @throws SwiftException
   */
  private URI pathToURI(SwiftObjectPath path) throws SwiftException {
    return pathToURI(path, getEndpointURI());
  }

  /**
   * Add the headers to the method, and the auth token (which must be set
   * 
   * @param method method to update
   * @param requestHeaders the list of headers
   * @throws SwiftInternalStateException not yet authenticated
   */
  private void setHeaders(HttpMethodBase method, Header[] requestHeaders) throws SwiftInternalStateException {
    for (Header header : requestHeaders) {
      method.addRequestHeader(header);
    }
    setAuthToken(method, getToken());
  }


  /**
   * Set the auth key header of the method to the token ID supplied
   *
   * @param method method
   * @param accessToken access token
   * @throws SwiftInternalStateException if the client is not yet authenticated
   */
  private void setAuthToken(HttpMethodBase method, AccessToken accessToken) throws SwiftInternalStateException {
    checkNotNull(accessToken, "Not authenticated");
    method.addRequestHeader(HEADER_AUTH_KEY, accessToken.getId());
  }

  /**
   * Execute a method in a new HttpClient instance. If the auth failed, authenticate then retry the method.
   *
   * @param method methot to exec
   * @param <M> Method type
   * @return the status code
   * @throws IOException on any failure
   */
  private <M extends HttpMethod> int exec(M method) throws IOException {
    final HttpClient client = initHttpClient(this.clientConfig);
    if (clientConfig.getProxyHost() != null) {
      client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(clientConfig.getProxyHost(), clientConfig.getProxyPort()));
    }

    int statusCode = -1;
    int retry = 0;
    do {
      if (retry > 0) {
        // re-auth, this may recurse into the same dir
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reauthenticating");
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
        authenticate();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Retrying original request");
        }
        // PGPBFDO-13963
        setAuthToken((HttpMethodBase) method, getToken());
      }
      statusCode = execWithDebugOutput(method, client);
      retry++;
    } while (statusCode == HttpStatus.SC_UNAUTHORIZED && retry < clientConfig.getRetryAuth());

    if ((statusCode == HttpStatus.SC_UNAUTHORIZED || statusCode == HttpStatus.SC_BAD_REQUEST) && method instanceof AuthPostMethod && !useKeystoneAuthentication) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Operation failed with status " + method.getStatusCode() + " attempting keystone auth");
      }
      // if rackspace key authentication failed - try custom Keystone authentication
      useKeystoneAuthentication = true;
      final AuthPostMethod authentication = (AuthPostMethod) method;
      // replace rackspace auth with keystone one
      authentication.setRequestEntity(getAuthenticationRequst(clientConfig.getKeystoneAuthRequest()));
      statusCode = execWithDebugOutput(method, client);
    }
    if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
      // unauthed -or the auth uri rejected it.
      // DTBFDTECH-261
      // if (method instanceof AuthPostMethod) {
      // unauth response from the AUTH URI itself.
      throw new SwiftAuthenticationFailedException(clientConfig.getAuthRequest().toString(), "auth", clientConfig.getAuthUri(), method);
      // }
      // any other URL: try again
      /**
       * if (LOG.isDebugEnabled()) { LOG.debug("Reauthenticating"); } // re-auth, this may recurse into the same dir authenticate(++exeCount); if (LOG.isDebugEnabled()) { LOG.debug("Retrying original
       * request"); } // PGPBFDO-13963 setAuthToken((HttpMethodBase) method, getToken()); statusCode = execWithDebugOutput(method, client);
       **/
    }
    return statusCode;
  }

  /**
   * Execute the request with the request and response logged at debug level
   * 
   * @param method method to execute
   * @param client client to use
   * @param <M> method type
   * @return the status code
   * @throws IOException any failure reported by the HTTP client.
   */
  private <M extends HttpMethod> int execWithDebugOutput(M method, HttpClient client) throws IOException {
    int statusCode = client.executeMethod(method);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Status code = " + statusCode);
    }
    return statusCode;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * @param reference an object reference
   * @return the non-null reference that was validated
   * @throws NullPointerException if {@code reference} is null
   */
  private static <T> T checkNotNull(T reference) throws SwiftInternalStateException {
    return checkNotNull(reference, "Null Reference");
  }

  private static <T> T checkNotNull(T reference, String message) throws SwiftInternalStateException {
    if (reference == null) {
      throw new SwiftInternalStateException(message);
    }
    return reference;
  }

  /**
   * Check for a status code being expected -takes a list of expected values
   *
   * @param status received status
   * @param expected expected value
   * @return true iff status is an element of [expected]
   */
  private boolean isStatusCodeExpected(int status, int... expected) {
    for (int code : expected) {
      if (status == code) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the current operation statistics
   * 
   * @return a snapshot of the statistics
   */

  public List<DurationStats> getOperationStatistics() {
    return durationStats.getDurationStatistics();
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }
}
