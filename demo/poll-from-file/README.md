Common operations demo app
==========================

INTRODUCTION
------------

This is a minimalistic demo of all the operations supported by HDFS Connector. It consists of below flows:

* *poll-from-file-flow* - This flow is responsible for polling file from time to time and log its content to console

HOW TO RUN DEMO
---------------

### Prerequisites
In order to build and run this demo project you'll need:

* Anypoint Studio with at least Mule ESB 3.6 Runtime.
* Mule HDFS Connector v5+
* An instance of HDFS server up and running.

### Test the flows

* Import the demo project into your workspace via "Anypoint Exchange" or "Import..." from "File" menu.
* From the Package Explorer view, expand the demo project and open the **mule-app.properties** file. Fill in the values for properties as explained below:
    * **config.nameNodeUri** - hostname of the machine where your HDFS server is deployed.
    * **config.sysUser** - (Optional) a simple user identity of a client process
    * **fileToPollFrom** - path to file that you want to poll content from
* Run the Mule Project.
* The content of file should be logged to console at a rate of 5 seconds which is default.

SUMMARY
-------

Congratulations! You have polled data from a file laying into HDFS server through Mule HDFS Connector.