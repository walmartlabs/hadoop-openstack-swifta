# bfd-ceph-swifta
A new CEPH swift driver that build on top of Sahara-extra.
## Usage:
1) Replace the value of "fs.swift.impl" in core-site.xml to "org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystem".

2) You may want to do the same for Hive, Spark or Presto if any core-site.xml presents.

3) Copy hadoop-ceph-2.8.0-SNAPSHOT.jar to $HADOOP_HOME/share/hadoop/tools/lib/ and link the same jar to $HADOOP_HOME/share/hadoop/hdfs/lib/

4) You are ready to go, make sure to use the same swift:// protocol.


This is a rough draft version, which still needs effort to do a full test, bug fixes, to extend the functionality, and to improve the performance.

#### This project only supports for Java 7 and above.
