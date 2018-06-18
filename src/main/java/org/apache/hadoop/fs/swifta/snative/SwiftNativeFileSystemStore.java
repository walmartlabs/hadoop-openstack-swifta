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



package org.apache.hadoop.fs.swifta.snative;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swifta.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swifta.http.HttpBodyContent;
import org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swifta.http.SwiftProtocolConstants.WritePolicies;
import org.apache.hadoop.fs.swifta.http.SwiftRestClient;
import org.apache.hadoop.fs.swifta.metrics.MetricsFactory;
import org.apache.hadoop.fs.swifta.model.ListObjectsRequest;
import org.apache.hadoop.fs.swifta.model.ObjectsList;
import org.apache.hadoop.fs.swifta.util.DurationStats;
import org.apache.hadoop.fs.swifta.util.JsonUtil;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;
import org.apache.hadoop.fs.swifta.util.SwiftUtils;
import org.apache.hadoop.fs.swifta.util.ThreadManager;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.map.type.CollectionType;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File system store implementation. Makes REST requests, parses data from responses.
 */
public class SwiftNativeFileSystemStore {

  private static final Header I_DIRECTORY = new Header(SwiftProtocolConstants.CONTENT_TYPE,
      SwiftProtocolConstants.CONTENT_TYPE_DIRECTORY);
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private static final Log LOG = LogFactory.getLog(SwiftNativeFileSystemStore.class);
  private static final MetricsFactory metric =
      MetricsFactory.getMetricsFactory(SwiftNativeFileSystemStore.class);
  private static final int MAX_LIMIT = 10000;
  private URI uri;
  private SwiftRestClient swiftRestClient;
  private DNSToSwitchMapping dnsToSwitchMapping;

  private static LFUCache<Header[]> cache1;
  private static LFUCache<Header[]> cache2;
  private static byte[] zeroByte = new byte[0];
  private Configuration conf;

  /**
   * Initialize the filesystem store -this creates the REST client binding.
   *
   * @param fsURI URI of the filesystem, which is used to map to the filesystem-specific options in
   *        the configuration file
   * @param configuration configuration
   * @throws IOException on any failure.
   */
  public void initialize(URI fsURI, final Configuration configuration) throws IOException {
    this.uri = fsURI;
    this.conf = configuration;
    dnsToSwitchMapping =
        ReflectionUtils.newInstance(configuration.getClass("topology.node.switch.mapping.impl",
            ScriptBasedMapping.class, DNSToSwitchMapping.class), configuration);
    this.swiftRestClient = new SwiftRestClient(fsURI, configuration);
    metric.increase(this);
    metric.report();
    if (cache1 == null) {
      cache1 = new LFUCache<Header[]>(swiftRestClient.getClientConfig().getCacheSize(),
          swiftRestClient.getClientConfig().getCacheLiveTime());
    }
    if (cache2 == null) {
      cache2 = new LFUCache<Header[]>(swiftRestClient.getClientConfig().getCacheSize(),
          swiftRestClient.getClientConfig().getCacheLiveTime());
    }

  }

  @Override
  public String toString() {
    return "SwiftNativeFileSystemStore with " + swiftRestClient;
  }

  public int getMaxInParallelUpload() {
    return this.swiftRestClient.getClientConfig().getMaxInParallelUpload();
  }

  /**
   * Create the swift output stream.
   * 
   * @param path path to write to
   * @return the new file
   * @throws IOException IOException
   */
  protected SwiftOutputStream createSwiftOutputStream(Path path) throws IOException {
    WritePolicies policy = this.swiftRestClient.getClientConfig().getWritePolicy();
    switch (policy) {
      case MULTIPART_NO_SPLIT:
        return new SwiftNativeOutputStreamMultipartNoSplit(this.conf, this, path.toUri().toString(),
            getPartsizeKb());
      case MULTIPART_SPLIT:
        return new SwiftNativeOutputStreamMultipartWithSplit(this.conf, this,
            path.toUri().toString(), getPartsizeKb(), getOutputBufferSize());
      case MULTIPART_SINGLE_THREAD:
        return new SwiftNativeOutputStreamMultiPartSingleThread(this.conf, this,
            path.toUri().toString(), getPartsizeKb());
      case MULTIPART_SPLIT_BLOCK:
        return new SwiftNativeOutputStreamMultipartWithSplitBlock(this.conf, this,
            path.toUri().toString(), getPartsizeKb(), getOutputBufferSize());
      default:
        return new SwiftNativeOutputStreamMultipartWithSplit(this.conf, this,
            path.toUri().toString(), getPartsizeKb(), getOutputBufferSize());
    }

  }

  /**
   * Get the default blocksize of this (bound) filesystem
   * 
   * @return the blocksize returned for all FileStatus queries, which is used by the MapReduce
   *         splitter.
   */
  public long getBlocksize() {
    return 1024L * swiftRestClient.getClientConfig().getBlocksizeKb();
  }

  public long getPartsizeKb() {
    return swiftRestClient.getClientConfig().getPartSizeKb();
  }

  public long getPartsizeBytes() {
    return swiftRestClient.getClientConfig().getPartSizeBytes();
  }

  public int getOutputBufferSize() {
    return swiftRestClient.getClientConfig().getOutputBufferSize();
  }

  public int getBufferSizeKb() {
    return swiftRestClient.getClientConfig().getBufferSizeKb();
  }

  public int getThrottleDelay() {
    return swiftRestClient.getClientConfig().getThrottleDelay();
  }

  /**
   * Upload a file/input stream of a specific length.
   *
   * @param path destination path in the swift filesystem
   * @param inputStream input data. This is closed afterwards, always
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFile(Path path, InputStream inputStream, long length) throws IOException {
    swiftRestClient.upload(toObjectPath(path), inputStream, length);
  }

  /**
   * Upload part of a larger file.
   *
   * @param path destination path
   * @param partNumber item number in the path
   * @param inputStream input data
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFilePart(Path path, int partNumber, InputStream inputStream, long length)
      throws IOException {

    String stringPath = path.toUri().getPath();
    String partitionFilename = SwiftUtils.partitionFilenameFromNumber(partNumber);
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(partitionFilename);
    } else {
      stringPath = stringPath.concat("/").concat(partitionFilename);
    }
    SwiftObjectPath p = new SwiftObjectPath(toDirPath(path).getContainer(), stringPath);
    swiftRestClient.upload(p, inputStream, length);
    if (LOG.isDebugEnabled()) {
      LOG.debug("[uploadFilePart] Uploaded a part to server, file length:" + length
          + " partNumber: " + partNumber + "" + p.toUriPath());
    }
  }

  /**
   * Tell the Swift server to expect a multi-part upload by submitting a 0-byte file with the
   * X-Object-Manifest header.
   *
   * @param path path of final final
   * @param fileLen file length
   * @throws IOException IOException
   */
  public void createManifestForPartUpload(Path path, long fileLen) throws IOException {
    SwiftObjectPath p = toObjectPath(path);
    String pathString = p.toUriPath();
    if (!pathString.endsWith("/")) {
      pathString = pathString.concat("/");
    }
    if (pathString.startsWith("/")) {
      pathString = pathString.substring(1);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Final writes x-object-manifest for header path:" + pathString);
    }
    swiftRestClient.upload(p, new ByteArrayInputStream(zeroByte), 0,
        new Header(SwiftProtocolConstants.X_OBJECT_MANIFEST, pathString),
        new Header(SwiftProtocolConstants.USER_CUSTOM_DATA, Long.toString(fileLen)));
  }

  /**
   * Tell the Swift server to expect a multi-part upload by submitting a 0-byte file with the
   * X-Object-Manifest header.
   *
   * @param path path of final final
   * @throws IOException IOException
   */
  public void createManifestForPartUpload(Path path) throws IOException {
    SwiftObjectPath p = toObjectPath(path);
    String pathString = p.toUriPath();
    if (!pathString.endsWith("/")) {
      pathString = pathString.concat("/");
    }
    if (pathString.startsWith("/")) {
      pathString = pathString.substring(1);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Final writes x-object-manifest for header path:" + pathString);
    }
    swiftRestClient.upload(p, new ByteArrayInputStream(zeroByte), 0,
        new Header(SwiftProtocolConstants.X_OBJECT_MANIFEST, pathString));
  }

  /**
   * Get the HTTP headers, in case you really need the low-level metadata.
   * 
   * @param path path to probe
   * @param newest newest or oldest?
   * @return the header list
   * @throws IOException IO problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public Header[] getObjectHeaders(Path path, boolean newest)
      throws IOException, FileNotFoundException {
    SwiftObjectPath objectPath = toObjectPath(path);
    return stat(objectPath, newest);
  }

  /**
   * Get the metadata of an object.
   *
   * @param path path
   * @return file metadata. -or null if no headers were received back from the server.
   * @throws IOException on a problem
   */
  public SwiftFileStatus getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(path, true);
  }

  /**
   * Get the metadata of an object.
   *
   * @param path path
   * @param newest flag to say "set the newest header", otherwise take any entry
   * @return file status
   * @throws IOException on a problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public SwiftFileStatus getObjectMetadata(Path path, boolean newest)
      throws IOException, FileNotFoundException {
    SwiftObjectPath objectPath = toObjectPath(path);
    final Header[] headers = stat(objectPath, newest);
    // no headers is treated as a missing file
    if (headers.length == 0) {
      throw new FileNotFoundException("Not Found " + path.toUri());
    }

    boolean isDir = false;
    long length = 0;
    long lastModified = 0;
    SwiftObjectPath objectManifest = null;
    for (Header header : headers) {
      String headerName = header.getName();
      if (SwiftProtocolConstants.CONTENT_TYPE.equals(headerName)) {
        isDir = SwiftProtocolConstants.CONTENT_TYPE_DIRECTORY.equals(header.getValue())
            ? Boolean.TRUE : Boolean.FALSE;
      }
      if (headerName.equals(SwiftProtocolConstants.X_CONTAINER_OBJECT_COUNT)
          || headerName.equals(SwiftProtocolConstants.X_CONTAINER_BYTES_USED)) {
        length = 0;
        isDir = true;
      }
      if (SwiftProtocolConstants.HEADER_CONTENT_LENGTH.equals(headerName)) {
        length = Long.parseLong(header.getValue());
      }
      if (SwiftProtocolConstants.HEADER_LAST_MODIFIED.equals(headerName)) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
        try {
          lastModified = simpleDateFormat.parse(header.getValue()).getTime();
        } catch (ParseException e) {
          throw new SwiftException("Failed to parse " + header.toString(), e);
        }
      }
      if (headerName.equals(SwiftProtocolConstants.X_OBJECT_MANIFEST)) {
        String[] values = header.getValue().split("/", 2);
        if (values.length == 2) {
          objectManifest = new SwiftObjectPath(values[0], "/" + values[1]);
        }
      }
    }
    if (lastModified == 0) {
      lastModified = System.currentTimeMillis();
    }
    if (SwiftUtils.isRootDir(objectPath)) {
      isDir = Boolean.TRUE;
    }
    return new SwiftFileStatus(length, isDir, 1, getBlocksize(), lastModified,
        getCorrectSwiftPath(path), objectManifest);
  }

  private Header[] stat(SwiftObjectPath objectPath, boolean newest) throws IOException {
    Header[] headers = null;
    if (newest) {
      headers = swiftRestClient.headRequest("getObjectMetadata-newest", objectPath,
          SwiftRestClient.NEWEST);
    } else {
      headers = swiftRestClient.headRequest("getObjectMetadata", objectPath);
    }
    return headers;
  }

  /**
   * Get the object as an input stream.
   *
   * @param path object path
   * @return the input stream -this must be closed to terminate the connection
   * @throws IOException IO problems
   * @throws FileNotFoundException path doesn't resolve to an object
   */
  public HttpBodyContent getObject(Path path) throws IOException {
    final List<String> locations = getDataLocalEndpoints(path);

    for (String url : locations) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading " + path + " from location: " + url);
      }
      try {
        return swiftRestClient.getData(new URI(url), SwiftRestClient.NEWEST);
      } catch (Exception e) {
        // Ignore
        // It is possible that end point doesn't contains needed data.
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading " + path + " from proxy node");
    }
    return swiftRestClient.getData(toObjectPath(path), SwiftRestClient.NEWEST);
  }



  /**
   * Get the input stream starting from a specific point.
   *
   * @param path path to object
   * @param byteRangeStart starting point
   * @param byteRangeEnd end index
   * @return an input stream that must be closed
   * @throws IOException IO problems
   */
  public HttpBodyContent getObject(Path path, long byteRangeStart, long byteRangeEnd)
      throws IOException {
    List<String> locations = getDataLocalEndpoints(path);

    for (String url : locations) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading " + path + " from location: " + url);
      }
      try {
        return swiftRestClient.getData(new URI(url), byteRangeStart, byteRangeEnd);
      } catch (Exception e) {
        // Ignore
      }
    }
    return swiftRestClient.getData(toObjectPath(path), byteRangeStart, byteRangeEnd);
  }

  /**
   * Returns list of endpoints for given swift path that are local for the host. List is returned in
   * order of preference.
   *
   * @param path path
   * @return list of locations
   * @throws IOException IOException
   */
  private List<String> getDataLocalEndpoints(Path path) throws IOException {
    final String hostRack = getHostRack();

    List<URI> uriLocations = getObjectLocation(path);
    List<String> strLocations = new ArrayList<String>();
    final Map<String, Integer> similarityMap = new HashMap<String, Integer>();
    for (URI uri : uriLocations) {
      String url = uri.getPath();
      int similarity = getSimilarity(getRack(uri.getHost()), hostRack);
      if (similarity > 0) {
        strLocations.add(url);
        similarityMap.put(url, similarity);
      }
    }

    Collections.sort(strLocations, new Comparator<String>() {
      public int compare(String o1, String o2) {
        Integer dst1 = similarityMap.get(o1);
        Integer dst2 = similarityMap.get(o2);
        return -dst1.compareTo(dst2);
      }
    });

    return strLocations;
  }

  /**
   * Returns similarity index for two racks. Bigger numbers correspond to closer location. Zero
   * corresponds to different racks.
   *
   * @param rack1 path to rack1
   * @param rack2 path to rack2
   * @return the similarity index
   */
  private int getSimilarity(String rack1, String rack2) {
    String[] r1 = rack1.split("/");
    String[] r2 = rack2.split("/");
    int i = 1; // skip leading empty string
    while (i < r1.length && i < r2.length && r1[i].equals(r2[i])) {
      i++;
    }

    return i - 1;
  }

  private String getHostRack() throws SwiftException {
    String hostAddress;
    try {
      hostAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new SwiftException("Failed to get localhost address", e);
    }
    return getRack(hostAddress);
  }

  private String getRack(String url) {
    // Temporary avoid the index issue.
    List<String> list = dnsToSwitchMapping.resolve(Arrays.asList(url));
    return list.size() == 0 ? "" : dnsToSwitchMapping.resolve(Arrays.asList(url)).get(0);
  }

  /**
   * List a directory. This is O(n) for the number of objects in this path.
   *
   * @param path working path
   * @param listDeep ask for all the data
   * @param newest ask for the newest data
   * @param marker where to start listing
   * @return Collection of file statuses
   * @throws IOException IO problems
   * @throws FileNotFoundException if the path does not exist
   */
  public ObjectsList listDirectory(SwiftObjectPath path, boolean listDeep, boolean newest,
      String marker) throws IOException {
    final ObjectsList objects = new ObjectsList();
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    final Path correctSwiftPath = getCorrectSwiftPath(path);
    byte[] bytes = null;
    try {
      bytes = swiftRestClient.listDeepObjectsInDirectory(path, listDeep, marker);
      if (LOG.isDebugEnabled()) {
        LOG.debug(new String(bytes) + " from path: " + path.toString());
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("" + "File/Directory not found " + path);
      }
      if (SwiftUtils.isRootDir(path)) {
        return objects;
      } else {
        throw e;
      }
    } catch (SwiftInvalidResponseException e) {
      if (LOG.isDebugEnabled()) {
        // Just for debug use.
        LOG.error(e.getMessage());
      }
      // bad HTTP error code
      if (e.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
        // this can come back on a root list if the container is empty
        if (SwiftUtils.isRootDir(path)) {
          return objects;
        } else {
          // NO_CONTENT returned on something other than the root directory;
          // see if it is there, and convert to empty list or not found
          // depending on whether the entry exists.
          FileStatus stat = getObjectMetadata(correctSwiftPath, newest);

          if (stat.isDirectory()) {
            // it's an empty directory. state that
            return null;
          } else {
            // it's a file -return that as the status
            files.add(stat);
            objects.setFiles(files);
            return objects;
          }
        }
      } else {
        // a different status code: rethrow immediately
        throw e;
      }
    }

    final CollectionType collectionType = JsonUtil.getJsonMapper().getTypeFactory()
        .constructCollectionType(List.class, SwiftObjectFileStatus.class);

    final List<SwiftObjectFileStatus> fileStatusList =
        JsonUtil.toObject(new String(bytes), collectionType);

    // this can happen if user lists file /data/files/file
    // in this case swift will return empty array
    if (fileStatusList.isEmpty()) {
      SwiftFileStatus objectMetadata = getObjectMetadata(correctSwiftPath, newest);
      if (LOG.isDebugEnabled()) {
        LOG.debug("The fileStatusList is empty:" + correctSwiftPath.toString());
      }
      if (objectMetadata.isFile()) {
        files.add(objectMetadata);
        objects.setFiles(files);
      }

      return objects;
    }
    if (fileStatusList.size() == MAX_LIMIT) {
      marker = fileStatusList.get(fileStatusList.size() - 1).getName();
    } else {
      marker = null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marker:" + marker);
    }
    long fileLen = 0;
    for (SwiftObjectFileStatus status : fileStatusList) {
      if (status.getName() != null) {
        fileLen = status.getFileLen() == null ? 0L : Long.parseLong(status.getFileLen());
        if (fileLen < 1) {
          files.add(new SwiftFileStatus(status.getBytes(), status.isDir(), 1, getBlocksize(),
              status.getLastModified().getTime(), getCorrectSwiftPath(new Path(status.getName()))));
        } else {
          files.add(new SwiftFileStatus(fileLen, Boolean.FALSE, 1, getBlocksize(),
              status.getLastModified().getTime(), getCorrectSwiftPath(new Path(status.getName()))));
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(fileStatusList.size() + " = page size. The marker is " + marker);
    }

    objects.setFiles(files);
    objects.setMarker(marker);
    return objects;
  }

  /**
   * List all elements in this directory.
   *
   * @param path path to work with
   * @param recursive do a recursive get
   * @param newest ask for the newest
   * @param marker for pagination
   * @return the file statuses, or an empty array if there are no children
   * @throws IOException on IO problems
   * @throws FileNotFoundException if the path is nonexistent
   */
  public ObjectsList listSubPaths(Path path, boolean recursive, boolean newest, String marker)
      throws IOException {
    return listDirectory(toDirPath(getCorrectSwiftPath(path)), recursive, newest, marker);
  }

  /**
   * Create a directory.
   *
   * @param path path
   * @throws IOException IOException
   */
  public void createDirectory(Path path) throws IOException {
    innerCreateDirectory(toDirPathCreate(path));
  }

  /**
   * Create a container.
   *
   * @param path container path
   * @throws IOException IOException
   */
  public void createContainer(Path path) throws IOException {
    SwiftObjectPath swiftObjectPath = toDirPath(path);
    String containerPath = swiftObjectPath.getContainer();
    swiftRestClient.createContainer(containerPath);
  }

  /**
   * Does the container of the path exist.
   * 
   * @param path container path
   * @return true if the container creation succeeds
   * @throws IOException IOException
   */
  public boolean doesExistContainer(Path path) throws IOException {
    SwiftObjectPath swiftObjectPath = toDirPath(path);
    String containerPath = swiftObjectPath.getContainer();
    return swiftRestClient.doesExistContainer(containerPath);
  }

  /**
   * The inner directory creation option. This only creates the dir at the given path, not any
   * parent dirs.
   * 
   * @param swiftObjectPath swift object path at which a 0-byte blob should be put
   * @throws IOException IO problems
   */
  private void innerCreateDirectory(SwiftObjectPath swiftObjectPath) throws IOException {
    swiftRestClient.putRequest(swiftObjectPath, I_DIRECTORY);
  }

  public SwiftObjectPath toDirPath(Path path) throws SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path, Boolean.FALSE);
  }

  public SwiftObjectPath toDirPathCreate(Path path) throws SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path, Boolean.FALSE, Boolean.TRUE);
  }

  private SwiftObjectPath toObjectPath(Path path) throws SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path);
  }

  /**
   * Try to find the specific server(s) on which the data lives.
   * 
   * @param path path to probe
   * @return a possibly empty list of locations
   * @throws IOException on problems determining the locations
   */
  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    objectLocation = swiftRestClient.getObjectLocation(toObjectPath(path));
    if (objectLocation == null || objectLocation.length == 0) {
      // no object location, return an empty list
      return new LinkedList<URI>();
    }
    return extractUris(new String(objectLocation), path);
  }

  /**
   * deletes object from Swift.
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation.
   * @throws IOException on a failure
   */
  public boolean deleteObject(Path path) throws IOException {
    SwiftObjectPath swiftObjectPath = toObjectPath(path);
    // if (!SwiftUtils.isRootDir(swiftObjectPath)) {
    this.clearCache(swiftObjectPath.toUriPath());
    return swiftRestClient.delete(swiftObjectPath);
    // } else {
    // if (LOG.isDebugEnabled()) {
    // LOG.debug("Not deleting root directory entry");
    // }
    // return true;
    // }
  }

  private void clearCache(String path) {
    if (swiftRestClient.getClientConfig().isUseHeaderCache()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("LRU cache: Deleting path " + path);
      }
      cache1.remove(path);
      cache2.remove(path);
    }
  }

  /**
   * deletes a batch of objects from Swift.
   *
   * @param statuses file details
   * @throws IOException on a failure
   */
  public void deleteObjects(List<FileStatus> statuses) throws IOException {
    Map<String, Future<Boolean>> deletes = new HashMap<String, Future<Boolean>>();
    ThreadManager tm = null;
    try {
      tm = this.getThreadManager(statuses.size(),
          this.swiftRestClient.getClientConfig().getMaxThreadsInPool());
      for (FileStatus entryStatus : statuses) {
        final Path entryPath = entryStatus.getPath();
        // boolean deleted = deleteObject(entryPath);
        deletes.put(entryPath.toUri().getPath(), tm.getPool().submit(new Callable<Boolean>() {
          public Boolean call() throws Exception {
            SwiftObjectPath swiftObjectPath = toObjectPath(entryPath);
            clearCache(swiftObjectPath.toUriPath());
            return swiftRestClient.delete(swiftObjectPath);
          }
        }));
        // throttle();
      }

      tm.shutdown();
      Set<Map.Entry<String, Future<Boolean>>> entrySet = deletes.entrySet();
      for (Map.Entry<String, Future<Boolean>> entry : entrySet) {
        String key = entry.getKey();
        Future<Boolean> future = entry.getValue();
        try {
          if (!future.get()) {
            LOG.info("Failed to delete entry '" + key + "'; continuing");
            // SwiftUtils.debug(LOG, "Failed to delete entry '%s'; continuing", key);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          // the path went away -race conditions.
          // do not fail, as the outcome is still OK.
          SwiftUtils.debug(LOG, "Path '%s' may no longer present; continuing", key);
        } finally {
          future.cancel(Boolean.TRUE);
        }
      }
    } finally {
      // free memory
      if (tm != null) {
        tm.cleanup();
        tm = null;
      }
      deletes.clear();
      deletes = null;
    }
  }

  /**
   * Is the file system location aware.
   *
   * @return true if the file system is location aware
   */
  public boolean isLocationAware() {
    return swiftRestClient.getClientConfig().isLocationAware();
  }

  /**
   * deletes a directory from Swift. This is not recursive.
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation
   * @throws IOException on a failure
   */
  public boolean rmdir(Path path) throws IOException {
    return deleteObject(path);
  }

  /**
   * Does the object exist.
   *
   * @param path object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which is downgraded to an object does
   *         not exist return code
   */
  public boolean objectExists(Path path) throws IOException {
    return objectExists(toObjectPath(path));
  }

  /**
   * Does the object exist.
   *
   * @param path swift object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which is downgraded to an object does
   *         not exist return code
   */
  public boolean objectExists(SwiftObjectPath path) throws IOException {
    try {
      Header[] headers = swiftRestClient.headRequest("objectExists", path, SwiftRestClient.NEWEST);
      // no headers is treated as a missing file
      return headers.length != 0;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  private ThreadManager getThreadManager(List<FileStatus> childStats) {
    return this.getThreadManager(childStats.size(),
        this.swiftRestClient.getClientConfig().getMaxThreadsForCopy());
  }

  private ThreadManager getThreadManager(int size, int max) {
    ThreadManager tm = new ThreadManager();
    // If required threads less than number in configuration, only picks less one.
    int maxThread = size > max ? max : size;
    tm.createThreadManager(maxThread);
    return tm;
  }

  /**
   * Rename through copy-and-delete. this is a consequence of the Swift filesystem using the path as
   * the hash into the Distributed Hash Table, "the ring" of filenames.
   * <p>
   * Because of the nature of the operation, it is not atomic.
   * </p>
   *
   * @param src source file/dir
   * @param dst destination
   * @throws IOException IO failure
   * @throws SwiftOperationFailedException if the rename failed
   * @throws FileNotFoundException if the source directory is missing, or the parent directory of
   *         the destination
   */
  public void rename(final Path src, final Path dst)
      throws FileNotFoundException, SwiftOperationFailedException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("mv " + src + " " + dst);
    }
    boolean renamingOnToSelf = src.equals(dst);

    SwiftObjectPath srcObject = toObjectPath(src);
    SwiftObjectPath destObject = toObjectPath(dst);

    if (SwiftUtils.isRootDir(srcObject)) {
      throw new SwiftOperationFailedException("cannot rename root dir");
    }

    final SwiftFileStatus srcMetadata;
    srcMetadata = getObjectMetadata(src);
    boolean isPartFile = srcMetadata.isPartitionedFile();
    if (LOG.isDebugEnabled()) {
      LOG.debug(src.toUri().getPath() + ": Is a partition file? " + isPartFile + "; moving to "
          + dst.toUri().getPath());
    }
    SwiftFileStatus dstMetadata;
    try {
      dstMetadata = getObjectMetadata(dst);
    } catch (FileNotFoundException e) {
      // destination does not exist.
      // LOG.error("Destination does not exist:" + dst);
      dstMetadata = null;
    }

    // check to see if the destination parent directory exists
    Path srcParent = src.getParent();
    Path dstParent = dst.getParent();
    if (dst.toUri().getPath().equals(srcParent.toUri().getPath())) {
      throw new SwiftOperationFailedException("src and dst refer to the same file or directory.");
    }
    // skip the overhead of a HEAD call if the src and dest share the same
    // parent dir (in which case the dest dir exists), or the destination
    // directory is root, in which case it must also exist
    if (dstParent != null && !dstParent.equals(srcParent)) {
      try {
        getObjectMetadata(dstParent);
      } catch (FileNotFoundException e) {
        // destination parent doesn't exist; bail out
        LOG.error("destination parent directory " + dstParent + " doesn't exist");
        throw e;
      }
    }

    boolean destExists = dstMetadata != null;
    boolean destIsDir = destExists && dstMetadata.isDir();
    // calculate the destination
    SwiftObjectPath destPath;

    SwiftObjectPath targetObjectPath = null;
    ListObjectsRequest request = new ListObjectsRequest(srcObject, true, true, this);
    // enum the child entries and everything underneath
    Iterator<ObjectsList> ite = request.iterator();
    while (ite.hasNext()) {
      ObjectsList object = ite.next();
      List<FileStatus> childStats = object.getFiles();
      boolean srcIsFile = !srcMetadata.isDirectory();
      if (LOG.isDebugEnabled() && isPartFile) {
        LOG.debug("Found partition file!" + src + ";len:" + srcMetadata.getLen());
      }
      if (srcIsFile) {

        // source is a simple file OR a partitioned file
        // outcomes:
        // #1 dest exists and is file: fail
        // #2 dest is a dir: fail
        // #3 doesn't exist: use dest as name
        destPath = toObjectPath(dst);
        if (destExists) {
          if (!destIsDir) {
            // outcome #1 dest it's a file: fail if differeent
            if (!renamingOnToSelf) {
              throw new SwiftOperationFailedException(
                  "Cannot rename a file over one that already exists.");
            } else {
              // is mv self self where self is a file. this becomes a no-op
              if (LOG.isDebugEnabled()) {
                LOG.debug("Renaming file onto self: no-op => success");
              }
              return;
            }

          } else {
            throw new SwiftOperationFailedException("Cannot rename a dir to a file.");
          }
        }
        int childCount = childStats.size();
        if (LOG.isDebugEnabled()) {
          LOG.debug("childCount:" + childCount);
        }
        // here there is one of:
        // - a single object ==> standard file
        // - it can contain the file itself
        if (childCount == 0 || childCount == 1) {
          copyThenDeleteObject(srcObject, destPath);
        } else {
          // do the copy
          SwiftUtils.debug(LOG,
              "Source file appears to be partitioned." + " copying file and deleting children");

          // copyObject(srcObject, destPath);
          // this.createManifestForPartUpload(getCorrectSwiftPath(destPath));

          final Path newPrefixPath = getCorrectSwiftPath(destObject);
          String newPrefixName = newPrefixPath.toUri().getPath();
          if (!newPrefixName.endsWith("/")) {
            newPrefixName = newPrefixName.concat("/");
          }
          ThreadManager tm = this.getThreadManager(childStats);
          Map<String, Future<Boolean>> copies = new HashMap<String, Future<Boolean>>();
          for (FileStatus s : childStats) {
            if (s.getLen() == 0) {
              continue;
            }
            String oldName = s.getPath().toUri().getPath();
            String oldPrefix = src.toUri().getPath();
            String suffix;
            if (oldPrefix.length() >= oldName.length()) {
              suffix = oldName.substring(oldName.lastIndexOf('/'));
            } else {
              suffix = oldName.substring(oldPrefix.length() + 1);
            }
            if (suffix.startsWith("/")) {
              suffix = suffix.substring(1);
            }
            String newName = newPrefixName + suffix;
            final SwiftObjectPath srcSeg = new SwiftObjectPath(srcObject.getContainer(), oldName);
            final SwiftObjectPath destSeg = new SwiftObjectPath(destObject.getContainer(), newName);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Moving file " + srcSeg.toUriPath() + " to " + destSeg.toUriPath());
            }
            copies.put(srcSeg.toUriPath(), tm.getPool().submit(new Callable<Boolean>() {
              public Boolean call() throws Exception {
                try {
                  if (!swiftRestClient.copyObject(srcSeg, destSeg)) {
                    throw new SwiftException("Copy of " + srcSeg + " to " + destSeg + "failed");
                  }
                  createManifestForPartUpload(newPrefixPath, srcMetadata.getLen());
                  return true;
                } catch (IOException e) {
                  return false;
                }
              }
            }));
          }

          tm.shutdown();
          this.waitToFinish(copies);

          this.clearCache(srcObject.toUriPath());
          deleteObjects(childStats);
          swiftRestClient.delete(srcObject);
          childStats.clear();
          childStats = null;
          copies = null;
        }
      } else {

        // here the source exists and is a directory
        // outcomes (given we know the parent dir exists if we get this far)
        // #1 destination is a file: fail
        // #2 destination is a dir or doesn't exist: use dest as name
        // #3 source is a partition file: use subdir of destination as name
        // #3 if the dest path is not == or under src: fail


        if (destExists && !destIsDir) {
          // #1 destination is a file: fail
          throw new SwiftOperationFailedException(
              "The source is a directory, but the destination is a file.");
        }
        Path targetPath;
        // #2 destination is a dir or doesn't exist: use dest as name
        targetPath = dst;
        targetObjectPath = toObjectPath(targetPath);
        // final check for any recursive operations
        if (srcObject.isEqualToOrParentOf(targetObjectPath)) {
          // you can't rename a directory onto itself
          throw new SwiftOperationFailedException("cannot move a directory under itself");
        }


        LOG.info("moving  " + srcObject + " " + targetPath);

        logDirectory("Directory to copy ", srcObject, childStats);

        // iterative copy of everything under the directory.
        // by listing all children this can be done iteratively
        // rather than recursively -everything in this list is either a file
        // or a 0-byte-len file pretending to be a directory.
        String srcURI = srcObject.toUriPath();
        int prefixStripCount = SwiftUtils.decodeUrl(srcObject.toUriPath()).length() + 1;
        Map<String, Future<Boolean>> copies = new HashMap<String, Future<Boolean>>();
        ThreadManager tm = this.getThreadManager(childStats);
        try {
          for (final FileStatus fileStatus : childStats) {
            final Path copySourcePath = fileStatus.getPath();
            final SwiftObjectPath copySrcPath = toObjectPath(copySourcePath);
            final String copyDestSubPath = copySrcPath.toUriPath().substring(prefixStripCount);
            final Path copyDestPath =
                new Path(targetPath, SwiftUtils.encodeUrlNoCheck(copyDestSubPath));
            if (LOG.isDebugEnabled()) {
              // trace to debug some low-level rename path problems; retained
              // in case they ever come back.
              LOG.debug("srcURI=" + srcURI + "; copySourceURI=" + copySrcPath.toUriPath()
                  + "; copyDestSubPath=" + copyDestSubPath + "; copyDestPath=" + copyDestPath);
            }
            final SwiftObjectPath copyDestination = toObjectPath(copyDestPath);
            copies.put(srcURI, tm.getPool().submit(new Callable<Boolean>() {
              public Boolean call() throws Exception {
                try {
                  copyThenDeleteObject(copySrcPath, copyDestination);
                  // Bug fix: DTBFDP-80.
                  if (fileStatus.getLen() > getPartsizeBytes()) {
                    createManifestForPartUpload(copyDestPath, fileStatus.getLen());
                  }
                  return true;
                } catch (IOException e) {
                  return false;
                }
              }
            }));
            // add a throttle delay
            // throttle();
          }
          tm.shutdown();
          this.waitToFinish(copies);

        } finally {
          // free memory
          tm.cleanup();
          tm = null;
          copies.clear();
          copies = null;
          childStats.clear();
          childStats = null;
        }

      }
    }
    // now rename self. If missing, create the dest directory and warn
    if (targetObjectPath != null && !SwiftUtils.isRootDir(srcObject)) {
      try {
        copyThenDeleteObject(srcObject, targetObjectPath);
      } catch (FileNotFoundException e) {
        // create the destination directory
        LOG.warn("Source directory deleted during rename", e);
        innerCreateDirectory(destObject);
      }
    }
  }

  private void waitToFinish(Map<String, Future<Boolean>> copies) {
    Set<Map.Entry<String, Future<Boolean>>> entrySet = copies.entrySet();
    for (Map.Entry<String, Future<Boolean>> entry : entrySet) {
      String key = entry.getKey();
      Future<Boolean> future = entry.getValue();
      try {
        if (!future.get()) {
          LOG.info("Skipping rename of " + key);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("Skipping rename of " + key);
      } finally {
        future.cancel(Boolean.FALSE);
      }
    }
  }

  /**
   * Debug action to dump directory statuses to the debug log.
   *
   * @param message explanation
   * @param objectPath object path (can be null)
   * @param statuses listing output
   */
  private void logDirectory(String message, SwiftObjectPath objectPath,
      Iterable<FileStatus> statuses) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(message + ": listing of " + objectPath);
      for (FileStatus fileStatus : statuses) {
        LOG.debug(fileStatus.getPath());
      }
    }
  }

  /**
   * Do the copy.
   * 
   * @param srcKey src
   * @param dstKey dest
   * @throws IOException IOException
   */
  public void copy(Path srcKey, Path dstKey) throws IOException {
    SwiftObjectPath srcObject = toObjectPath(srcKey);
    SwiftObjectPath destObject = toObjectPath(dstKey);
    swiftRestClient.copyObject(srcObject, destObject);
  }


  /**
   * Copy an object then, if the copy worked, delete it. If the copy failed, the source object is
   * not deleted.
   *
   * @param srcObject source object path
   * @param destObject destination object path
   * @throws IOException IO problems
   * 
   */
  private void copyThenDeleteObject(SwiftObjectPath srcObject, SwiftObjectPath destObject)
      throws IOException {


    // do the copy
    copyObject(srcObject, destObject);
    // getting here means the copy worked
    this.clearCache(srcObject.toUriPath());
    swiftRestClient.delete(srcObject);
  }

  /**
   * Copy an object.
   * 
   * @param srcObject source object path
   * @param destObject destination object path
   * @throws IOException IO problems
   */
  private void copyObject(SwiftObjectPath srcObject, SwiftObjectPath destObject)
      throws IOException {
    if (srcObject.isEqualToOrParentOf(destObject)) {
      throw new SwiftException("Can't copy " + srcObject + " onto " + destObject);
    }
    // do the copy
    boolean copySucceeded = swiftRestClient.copyObject(srcObject, destObject);
    if (!copySucceeded) {
      throw new SwiftException("Copy of " + srcObject + " to " + destObject + "failed");
    }
  }

  /**
   * Take a Hadoop path and return one which uses the URI prefix and authority of this FS. It
   * doesn't make a relative path absolute.
   * 
   * @param path path in
   * @return path with a URI bound to this FS
   * @throws SwiftException URI cannot be created.
   */
  public Path getCorrectSwiftPath(Path path) throws SwiftException {
    try {
      return new Path(
          new URI(uri.getScheme(), uri.getAuthority(), path.toUri().getPath(), null, null));
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }

  /**
   * Builds a hadoop-Path from a swift path, inserting the URI authority of this FS instance.
   * 
   * @param path swift object path
   * @return Hadoop path
   * @throws SwiftException if the URI couldn't be created.
   */
  private Path getCorrectSwiftPath(SwiftObjectPath path) throws SwiftException {
    try {
      final URI fullUri =
          new URI(uri.getScheme(), uri.getAuthority(), path.getObject(), null, null);

      return new Path(fullUri);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }


  /**
   * Extracts URIs from json.
   * 
   * @param json json to parse
   * @param path path (used in exceptions)
   * @return URIs
   * @throws SwiftOperationFailedException on any problem parsing the JSON
   */
  public static List<URI> extractUris(String json, Path path) throws SwiftOperationFailedException {
    final Matcher matcher = URI_PATTERN.matcher(json);
    final List<URI> result = new ArrayList<URI>();
    while (matcher.find()) {
      final String s = matcher.group();
      final String uri = s.substring(1, s.length() - 1);
      try {
        URI createdUri = URI.create(uri);
        result.add(createdUri);
      } catch (IllegalArgumentException e) {
        // failure to create the URI, which means this is bad JSON. Convert
        // to an exception with useful text
        throw new SwiftOperationFailedException(String.format(
            "could not convert \"%s\" into a URI." + " source: %s " + " first JSON: %s", uri, path,
            json.substring(0, 256)));
      }
    }
    return result;
  }

  /**
   * Get the current operation statistics.
   * 
   * @return a snapshot of the statistics
   */
  public List<DurationStats> getOperationStatistics() {
    return swiftRestClient.getOperationStatistics();
  }


  /**
   * Delete the entire tree. This is an internal one with slightly different behavior: if an entry
   * is missing, a {@link FileNotFoundException} is raised. This lets the caller distinguish a file
   * not found with other reasons for failure, so handles race conditions in recursive directory
   * deletes better.
   * <p>
   * The problem being addressed is: caller A requests a recursive directory of directory /dir ;
   * caller B requests a delete of a file /dir/file, between caller A enumerating the files
   * contents, and requesting a delete of /dir/file. We want to recognise the special case "directed
   * file is no longer there" and not convert that into a failure
   * </p>
   * 
   * @param absolutePath the path to delete.
   * @param recursive if path is a directory and set to true, the directory is deleted else throws
   *        an exception if the directory is not empty case of a file the recursive can be set to
   *        either true or false.
   * @return true if the object was deleted
   * @throws IOException IO problems
   * @throws FileNotFoundException if a file/dir being deleted is not there - this includes entries
   *         below the specified path, (if the path is a dir and recursive is true)
   */
  public boolean delete(Path absolutePath, boolean recursive) throws IOException {
    Path swiftPath = getCorrectSwiftPath(absolutePath);
    SwiftUtils.debug(LOG, "Deleting path '%s' recursive=%b", absolutePath, recursive);
    boolean askForNewest = true;
    SwiftFileStatus fileStatus = getObjectMetadata(swiftPath, askForNewest);

    ListObjectsRequest request = new ListObjectsRequest(absolutePath, true, true, this);

    // ask for the file/dir status, but don't demand the newest, as we
    // don't mind if the directory has changed
    // list all entries under this directory.
    // this will throw FileNotFoundException if the file isn't there
    Iterator<ObjectsList> ite = request.iterator();
    while (ite.hasNext()) {
      ObjectsList object = ite.next();
      List<FileStatus> statuses = null;
      if (object != null) {
        statuses = object.getFiles();
      }
      try {
        if (statuses == null) {
          // the directory went away during the non-atomic stages of the operation.
          // Return false as it was not this thread doing the deletion.
          SwiftUtils.debug(LOG, "Path '%s' has no status -it has 'gone away'", absolutePath,
              recursive);
          return false;
        }
        int filecount = statuses.size();
        // SwiftUtils.debug(LOG, "Path '%s' %d status entries'", absolutePath, filecount);

        if (filecount == 0) {
          // it's an empty directory or a path
          rmdir(absolutePath);
          return true;
        }

        if (filecount == 1 && swiftPath.equals(statuses.get(0).getPath())) {
          // 1 entry => simple file and it is the target
          // simple file: delete it
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting simple file " + absolutePath);
          }
          // SwiftUtils.debug(LOG, "Deleting simple file %s", absolutePath);
          deleteObject(absolutePath);
          return true;
        }

        // >1 entry implies directory with children. Run through them,
        // but first check for the recursive flag and reject it *unless it looks
        // like a partitioned file (len > 0 && has children)
        if (!fileStatus.isDirectory()) {
          LOG.debug("Multiple child entries but entry has data: assume partitioned");
        } else if (!recursive) {
          // if there are children, unless this is a recursive operation, fail immediately
          throw new SwiftOperationFailedException("Directory " + fileStatus + " is not empty: "
              + SwiftUtils.fileStatsToString(statuses, "; "));
        }

        // delete the entries. including ourself.
        this.deleteObjects(statuses);
      } finally {
        statuses.clear();
        statuses = null;
      }
    }

    // now delete self
    SwiftUtils.debug(LOG, "Deleting base entry %s", absolutePath);
    deleteObject(absolutePath);
    return true;
  }

  public boolean isLazyseek() {
    return this.swiftRestClient.getClientConfig().isLazySeek();
  }

}
