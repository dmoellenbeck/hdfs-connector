HDFS Anypoint Connector Release Notes
=====================================

Date: 15-May-2014

Version: 3.7.1

Supported API versions: [v1.x](http://hadoop.apache.org/docs/r1.2.1/api/)

Supported Mule Runtime Versions: 3.4.x, 3.5.0

Features and functionality
--------------------------

* Create/delete directories
* Read/write from path
* Get path metadata
* Append to path

Known issues
------------

* Appending to paths has been disabled since Hadoop 1.1.0. See this [Hadoop issue](https://issues.apache.org/jira/browse/HADOOP-8230)
* Reading from paths is currently broken
