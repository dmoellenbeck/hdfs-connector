HDFS Connector Release Notes
=====================================

Date: 31-Oct-2014

Version: 4.0.1

Supported Mule Runtime Versions: 3.5.x

Supported API versions
----------------------
[Hadoop Client ver 2.5.0](http://hadoop.apache.org/docs/r2.5.0/api/)


New Features and Functionality
------------------------------
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

Read from path operation is now an inbound endpoint that can stream data. New operations added are Copy from Local File, Copy to Local File, Glob Status, List Status, Rename, Set Owner
& Set Permission.

Closed Issues in this release
-----------------------------

* Updated the Hadoop Java Client to support 2.5.x hosts.
* Appending to paths has been fixed.
* Reading from paths has been fixed.

Known Issues in this release
----------------------------

* Currently reading from paths does not support configuration for polling and move functionality.
* Functional test cases for Append & Read have been annotated @Ignore as they fail due to lack of additional ssh proxy configuration required to execute on Amazon EC2 Instance.
