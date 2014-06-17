HDFS Anypoint Connector Release Notes
=====================================

Date: 15-Jun-2014

Version: 3.8.0

Supported API versions: [v2.4.0](http://hadoop.apache.org/docs/r2.4.0/api/)

Supported Mule Runtime Versions: 3.5.0

Features and Functionality
--------------------------

* Create/delete directories
* Read/write from path
* Get path metadata
* Append to path

Closed Issues in this release
-----------------------------

* Updated the HDFS Java Client to support 2.4.x hosts.
* Appending to paths has been fixed.
* Reading from paths has been fixed.

Known Issues in this release
----------------------------

* Currently reading from paths does not support polling and move functionality.
* Functional test cases for Append & Read have been annotated @Ignore.
