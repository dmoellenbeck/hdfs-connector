HDFS Connector Release Notes
=====================================

Date: 25-Jun-2014

Version: 4.0.0

Supported Mule Runtime Versions: 3.5.0

Supported API versions
----------------------
[Hadoop Client ver 2.4.0](http://hadoop.apache.org/docs/r2.4.0/api/)

Supported Operations:

* Append to File
* Copy from Local File
* Copy to Local File
* Delete Directories
* Delete File
* Get Path Meta Data
* Glob Status
* List Status
* Make Directories
* Rename
* Set Owner
* Set Permission
* Write to Path
* Read from Path

New Features and Functionality
------------------------------

Read from path operation is now an inbound endpoint that can stream data. The connector now supports 2.4.x Hadoop hosts 
and new operations like Copy from Local File, Copy to Local File, Glob Status, List Status, Rename, Set Owner 
& Set Permission are supported. 

Closed Issues in this release
-----------------------------

* Updated the HDFS Java Client to support 2.4.x hosts.
* Appending to paths has been fixed.
* Reading from paths has been fixed.

Known Issues in this release
----------------------------

* Currently reading from paths does not support configuration for polling and move functionality.
* Functional test cases for Append & Read have been annotated @Ignore.
