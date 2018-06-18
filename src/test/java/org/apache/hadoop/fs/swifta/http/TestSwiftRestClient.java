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


package org.apache.hadoop.fs.swifta.http;

import org.apache.commons.httpclient.Header;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swifta.SwiftTestConstants;
import org.apache.hadoop.fs.swifta.http.SwiftRestClient;
import org.apache.hadoop.fs.swifta.util.Duration;
import org.apache.hadoop.fs.swifta.util.DurationStats;
import org.apache.hadoop.fs.swifta.util.SwiftObjectPath;
import org.apache.hadoop.fs.swifta.util.SwiftTestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class TestSwiftRestClient implements SwiftTestConstants {
  private static final Log LOG = LogFactory.getLog(TestSwiftRestClient.class);

  private Configuration conf;
  private boolean runTests;
  private URI serviceURI;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    runTests = SwiftTestUtils.hasServiceUri(conf);
    if (runTests) {
      serviceURI = SwiftTestUtils.getServiceUri(conf);
    }
  }

  protected void assumeEnabled() {
    Assume.assumeTrue(runTests);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCreate() throws Throwable {
    assumeEnabled();
    createClient();
  }

  private SwiftRestClient createClient() throws IOException {
    return SwiftRestClient.getInstance(serviceURI, conf);
  }


  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testAuthenticate() throws Throwable {
    assumeEnabled();
    SwiftRestClient client = createClient();
    client.authenticate();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPutAndDelete() throws Throwable {
    assumeEnabled();
    SwiftRestClient client = createClient();
    client.authenticate();
    Path path = new Path("restTestPutAndDelete");
    SwiftObjectPath sobject = SwiftObjectPath.fromPath(serviceURI, path);
    byte[] stuff = new byte[1];
    stuff[0] = 'a';
    client.upload(sobject, new ByteArrayInputStream(stuff), stuff.length);
    // check file exists
    Duration head = new Duration();
    Header[] responseHeaders =
        client.headRequest("expect success", sobject, SwiftRestClient.NEWEST);
    head.finished();
    LOG.info("head request duration " + head);
    for (Header header : responseHeaders) {
      LOG.info(header.toString());
    }
    // delete the file
    client.delete(sobject);
    // check file is gone
    try {
      client.headRequest("expect fail", sobject, SwiftRestClient.NEWEST);
      Assert.fail("Expected deleted file, but object is still present: " + sobject);
    } catch (FileNotFoundException e) {
      // expected
    }
    for (DurationStats stats : client.getOperationStatistics()) {
      LOG.info(stats);
    }
  }

}
