# bfd-ceph-swifta
A new CEPH swift driver that based on Sahara-extra.
## Usage:
1) Replace the value of "fs.swift.impl" in core-site.xml to "org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystem".

2) You may want to do the same for Hive, Spark or Presto if any core-site.xml presents.

3) Copy hadoop-ceph-2.8.0-SNAPSHOT.jar to $HADOOP_HOME/share/hadoop/tools/lib/ and link the same jar to $HADOOP_HOME/share/hadoop/hdfs/lib/

4) You are ready to go, make sure to use the same swift:// protocol.


This branch adds a content-type to all dummy folders. For all files, even 0-byte one stays empty for the content_type.
#### This project only supports for Java 7 and above.
