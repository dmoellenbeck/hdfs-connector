Common operations demo app
==========================

INTRODUCTION
------------

This is a minimalistic demo of all the operations supported by HDFS Connector. It consists of below flows:

* *UI_Provider_Flow* - This flow is responsible for providing a nice web page when the user is hitting "localhost:8081" from its favorite browser.
* *Delete_File_Flow* - This flow is responsible for calling "Delete file" operation with params provided.  
* *Create_File_Flow* - This flow is responsible for calling "Write to path" operation with params provided.
* *Append_File_Flow* - This flow is responsible for calling "Append to file" operation with params provided.
* *Delete_Directory_Flow* - This flow is responsible for calling "Delete directory" operation with params provided.
* *Create_Directory_Flow* - This flow is responsible for calling "Make directory" operation with params provided.
* *Metadata_Flow* - This flow is responsible for calling "Get path meta data" operation with params provided.
* *Copy_From_Local_File_Flow* - This flow is responsible for calling "Copy from local file" operation with params provided.
* *Copy_To_Local_File_Flow* - This flow is responsible for calling "Copy to local file" operation with params provided.
* *Rename_Flow* - This flow is responsible for calling "Rename" operation with params provided.
* *Set_Permission_Flow* - This flow is responsible for calling "Get permission" operation with params provided.
* *Set_Owner_Flow* - This flow is responsible for calling "Set owner" operation with params provided.
* *List_Status_Flow* - This flow is responsible for calling "List status" operation with params provided.
* *Glob_Status_Flow* - This flow is responsible for calling "Glob status" operation with params provided.
* *Read_From_File_Flow* - This flow is responsible for calling "Read from path" operation with params provided.

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
* Run the Mule Project.
* From your favorite browser access http://localhost:8090
* A webpage from which you can play with different HDFS operation is going to be displayed.

SUMMARY
-------

Congratulations! You have ran different commands against HDFS server through Mule HDFS Connector.

