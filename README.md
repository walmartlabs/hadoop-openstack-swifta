# hadoop-openstack-swifta

This module enables Apache Hadoop applications including MapReduce jobs, read and write data to and from instances of the OpenStack Swift object store. It significantly rewrites the existing hadoop-openstack swift driver over the icehouse release of openstack sahara-extra project: https://github.com/openstack/sahara-extra/tree/icehouse-eol. It can be embedded into the the hadoop-openstack submodule of the hadoop codebase: https://github.com/apache/hadoop/tree/trunk/hadoop-tools/hadoop-openstack, in a way very similar to the huge efforts of the hadoop-aws s3a over s3n enhancements: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html. This codebase has been tested extensively against Swift-API compatible Ceph Jewel 10.2.7 object storage. 

## How to build and test

The hadoop-openstack-swifta codebase can be remotely tested against any public or private cloud infrastructure which supports the OpenStack Keystone authentication mechanism. It can also be tested against private OpenStack clusters. OpenStack Development teams are strongly encouraged to test the Hadoop swift filesystem client against any version of Swift that they are developing or deploying, to stress their cluster and to identify bugs early.

The module comes with a large suite of JUnit tests -tests that are only executed if the source tree includes credentials to test against a specific cluster.

Create the file: 
     
     src/test/resources/auth-keys.xml
     
Into this file, insert the credentials needed to bond to the test filesystem, as decribed above.

Next set the property test.fs.swifta.name to the URL of a swift container to test against. The tests expect exclusive access to this container -do not keep any other data on it, or expect it to be preserved.

    <property>
      <name>test.fs.swifta.name</name>
      <value>swifta://test.myswift/</value>
    </property>
    
Build swifta package:

     mvn clean install -DskipTests
   
This builds a set of Hadoop JARs consistent with the hadoop-openstack module that is about to be tested.

     mvn test -Dtest=TestSwiftRestClient
   
This runs some simple tests which include authenticating against the remote swift service. If these tests fail, so will all the rest. If it does fail: check your authentication.

Once this test succeeds, you can run the full test suite:

     mvn test
  

## How to configurae a hadoop cluster with swifta:

1) Build swifta: mvn clean install -DskipTests

2) Add the value of "fs.swifta.impl" in core-site.xml to "org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystem".

3) Copy hadoop-openstack-*.jar to $HADOOP_HOME/share/hadoop/tools/lib/ and link the same jar to $HADOOP_HOME/share/hadoop/common/lib/

4) You are ready to go, make sure to use the swifta:// protocol, e.g.: hadoop fs -ls swifta://test.myswift/

