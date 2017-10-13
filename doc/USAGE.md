[Purpose](#purpose)

[Prerequisites](#prerequisites)

[Getting Anypoint Studio Ready](#getting-anypoint-studio-ready)

[Setting up the project](#setting-up-the-project)

[Building the flows](#building-the-flows)

[Other resources](#other-resources)

###Purpose

This document provides detailed instructions on how to install MuleSoft's HDFS connector and demonstrates how to build and run a simple demo application with Mule that creates a directory on HDFS, retrieves metadata about the directory, and deletes the directory.

###Prerequisites

In order to build and run this project you'll need,

*   A working [Apache Hadoop Server](http://hadoop.apache.org/).
*   Downloaded and installed [Anypoint Studio Community edition](http://www.mulesoft.org/download-mule-esb-community-edition).

###Getting Anypoint Studio Ready

If you haven't installed Anypoint Studio on your computer yet, it's time to download Anypoint Studio from this location: [http://www.mulesoft.org/download-mule-esb-community-edition](http://www.mulesoft.org/download-mule-esb-community-edition). You also have the option of downloading a 30 day trial of Mule Enterprise Edition from this location [http://www.mulesoft.com/mule-esb-enterprise](http://www.mulesoft.com/mule-esb-enterprise) if you want to evaluate and purchase the premium edition. This demo can be built using either community or enterprise edition. There is no specific installation that you need to run. Once you unzip the zip file to your desired location, you are ready to go. To install the HDFS connector, you can download and install it from Anypoint Connectors maven dependencies.

1.Copy the related dependency from the location specified above and paste it in the Maven's pom file present in your project directory between <dependencies></dependencies> tags.

![](images/image004.jpg)

###Setting up the project

Now that you've got your Anypoint Studio up and running, it's time to work on the Mule App. Create a new Mule Project by clicking on "File \> New \> Mule Project". In the new project dialog box, the only thing you are required to enter is the name of the project. You can click on "Next" to go through the rest of pages.

The first thing to do in your new app is to configure the connection to HDFS. In the message flow editor, click on "Global Elements" tab on the bottom of the page. Then click on "Create" button on the top right of the tab. In the "Choose Global Element" type dialog box that opens select "HDFS" under "Connector Configuration" and click okay.

![](images/hdfsCreateConfigRef.png)

In the HDFS Configuration box that follows, set the NameNode URI to the name and port for your HDFS server. For example: "hdfs://localhost:9000"

![](images/hdfsGlobalElement.png)

The XML for the global element should look like this:

     <hdfs:config name="hdfs-conf" nameNodeUri="hdfs://localhost:9000" username="hduser" doc:name="HDFS"/>

###Building the flows

It's time to build the flows which create a directory, retrieve metadata about the directory, and delete the directory on HDFS.

![](images/Create_Directory_Flow.png)

**Create Directory flow:** This is the flow which creates a directory on the HDFS server. Start by dragging an HTTP endpoint from the palette onto the flow. Configure the Host, Port, and Path to "localhost", "8081", and "dircreate", respectively. This is the URL you will call to start the flow.
Then drag an HDFS Connector onto the flow after the HTTP endpoint. In the configuration window for the HDFS connector, select the previously created HDFS config from the Config Reference dropdown. Set the Operation to "Make directories", and set the Path field to "#[attributes.queryParams.path]". Click okay.

This completes the Create Directory flow.

![](images/Meta_Data_Flow.png)

**Directory Metadata flow:** This is the flow which retrieves the metadata information about a particular directory. Start by dragging an HTTP endpoint from the palette onto the workspace (not onto a flow), creating a new flow. Configure the Host, Port, and Path to "localhost", "8081", and "metadata", respectively. This is the URL you will call to start the flow.
Then drag an HDFS Connector onto the flow after the HTTP endpoint. In the configuration window for the HDFS connector, select the previously created HDFS config from the Config Reference dropdown. Set the Operation to "Get path meta data", and set the Path field to "#[attributes.queryParams.path]". Click okay.

Now drag a Transform Message onto the flow after the HDFS Connector. In the configuration window for the Set Payload Transformer, set the Value field to

<![CDATA[%dw 2.0
output application/json
---

payload 
]]></ee:set-payload>
		
and click okay.  

This completes the Directory Metadata flow.

![](images/Delete_Directory_Flow.png)

**Delete Directory flow:** This is the flow which deletes a directory on the HDFS server. Start by dragging an HTTP endpoint from the palette onto the flow. Configure the Host, Port, and Path to "localhost", "8081", and "dirdelete", respectively. This is the URL you will call to start the flow.
Then drag an HDFS Connector onto the flow after the HTTP endpoint. In the configuration window for the HDFS connector, select the previously created HDFS config from the Config Reference dropdown. Set the Operation to "Delete directory", and set the Path field to  "#[attributes.queryParams.path]". Click okay.

**Flow XML**

The final flow XML should look like this.

    <xmlns:http="http://www.mulesoft.org/schema/mule/http"
     xmlns:hdfs="http://www.mulesoft.org/schema/mule/hdfs"
     	xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
     	xmlns="http://www.mulesoft.org/schema/mule/core" xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
     	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     	xsi:schemaLocation="
     	
     http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd 
     http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
     http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
     http://www.mulesoft.org/schema/mule/hdfs http://www.mulesoft.org/schema/mule/hdfs/current/mule-hdfs.xsd">
     
     
         <hdfs:hdfs-config name="hdfs-conf" doc:name="HDFS">
    	         <hdfs:simple-connection nameNodeUri="hdfs://localhost:9000" username="hduser"/>
         </hdfs:hdfs-config>
    
    	<http:listener-config name="HTTP_Listener_config" doc:name="HTTP Listener config" doc:id="6ae5b064-a085-4c94-a5ee-a379f07cf81a" >
        		<http:listener-connection host="localhost" port="8081" />
        </http:listener-config>
        	
        <flow name="Create_File_Flow">
                           <http:listener config-ref="HTTP_Listener_config" path="/filecreate" doc:name="HTTP"/>
                           <logger message="#['Creating file  : ' ++ attributes.queryParams.path ++ '  with message : ' ++ attributes.queryParams.content]"
                                   level="INFO" doc:name="Write to Path Log"/>
                           <set-payload value="#[attributes.queryParams.content]" doc:name="Set the message input as payload"/>
                           <hdfs:write config-ref="hdfs-conf" path="#[attributes.queryParams.path]" permission="755"
                                       doc:name="Write to Path"/>
         </flow>
         
           <flow name="Metadata_Flow">
           <http:listener config-ref="HTTP_Listener_config" path="/metadata" doc:name="HTTP"/>
               
          <hdfs:get-metadata config-ref="hdfs-conf" path="#[attributes.queryParams.path]"
                                               doc:name="Get Path Meta Data" 
        
                    		<ee:transform doc:name="Transform Message" doc:id="ebf0da7d-1d1a-4324-a0e6-618bb9191387" >
                    			<ee:message >
                    				<ee:set-payload ><![CDATA[%dw 2.0
                    output application/json
                    ---
                    
                    payload 
                    ]]></ee:set-payload>
                    			</ee:message>
                    		</ee:transform>
                           
                        
          </flow>
                       
                       
       <flow name="Delete_File_Flow">
               <http:listener config-ref="HTTP_Listener_config" path="/filedelete" doc:name="HTTP"/>
               <logger message="#[attributes.queryParams.path]" level="INFO" doc:name="Delete file log"/>
               <hdfs:delete-file config-ref="hdfs-conf" path="#[attributes.queryParams.path]" doc:name="Delete file"/>
               <set-payload value='#[attributes.queryParams.path ++ " succesfully deleted"]' doc:name="Set Payload"/>
           </flow>
           
          
    </mule>


**Testing the app**

Now it's time to test the app. Run the app in Anypoint Studio and open a browser window. Visit [http://localhost:8081/dircreate?path=muledir](http://localhost:8081/dircreate?path=muledir). This will create a directory on the HDFS server.
Now visit [http://localhost:8081/metadata?path=muledir](http://localhost:8081/metadata?path=muledir). This will retrieve metadata about the recently created directory.
Now visit [http://localhost:8081/dirdelete?path=muledir](http://localhost:8081/dirdelete?path=muledir). This will delete the previously created directory.

###Other resources

For more information on:

●     Mule platform and how to build Mule apps, please visit  [http://www.mulesoft.org/documentation](http://www.mulesoft.org/documentation/display/current/Home)
