# bfd-ceph-swifta
A Hadoop Swift-API compatible file system driver, based on sahara-extra, that is tested against OpenStack Ceph. 
## Usage:
1) Add the value of "fs.swifta.impl" in core-site.xml to "org.apache.hadoop.fs.swifta.snative.SwiftNativeFileSystem".

2) You may want to do the same for Hive, Spark or Presto if any core-site.xml presents.

3) Copy hadoop-openstack-*.jar to $HADOOP_HOME/share/hadoop/tools/lib/ and link the same jar to $HADOOP_HOME/share/hadoop/hdfs/lib/

4) You are ready to go, make sure to use the same swifta:// protocol.


This branch adds a content-type to all dummy folders. For all files, even 0-byte one stays empty for the content_type.
#### This project only supports for Java 7 and above.
