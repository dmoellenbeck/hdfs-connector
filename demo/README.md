HDFS STUDIO DEMO
================

INTRODUCTION
------------

This is a minimalistic demo of all the operations supported by HDFS Connector.

HOW TO EXECUTE THE DEMO
-----------------------

1. Import your project into Studio.
2. Enter your HDFS instance credentials.
3. Run the Mule Project.
4. Prepare the URL based on the flow, for example to run Delete File Flow prepare the URL as http://localhost:8090/filedelete?path=.
5. Execute the prepared URL in browser.

HOW TO RUN RETRIEVE FILE FLOW
-----------------------------

The Retrieve_File_Flow consist of an inbound message source that constantly polls for file changes. To avoid exceptions and confusion, the
flows initialState has been set to stopped. The prerequisite to run this flow is to create a file on the Hadoop Distributed FileSystem and
replace the "#[payload]" with the path of the file in the below tag.

<hdfs:read config-ref="hdfs-conf" path="#[payload]" doc:name="Read From Path"/>

Once the changes are done, set the initialState of the flow from stopped to started. The flow will then start to read the file.


HOW TO DEFINE A PATHFILTER
---------------------------
PathFilter provides enough flexibility to describe a set of files you want to access, which cannot be done by glob patterns at times. PathFilter is for
Path objects. The implementation of PathFilter is shown in RegexIncludePathFilter.java, that implements a PathFilter for including paths that match a regular expression.
The filter passes only those files that match the regular expression.

