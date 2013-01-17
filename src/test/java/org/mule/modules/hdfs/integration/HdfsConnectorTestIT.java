/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.integration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.api.ConnectionException;
import org.mule.modules.hdfs.HdfsConnector;

public class HdfsConnectorTestIT {
	
	static private final Log logger = LogFactory.getLog(HdfsConnectorTestIT.class);
	
	private String filesystemname;
	private String username;
	private String usergroup;
	private String filename;
	private String dir;
	private String permission;
	private String encoding;
	private String messagepart1;
	private String messagepart2;
	private HdfsConnector conn;
	
	@Before
    public void setUp() throws IOException, ConnectionException{
		// Load the .properties
		Properties prop = new Properties();
		ClassLoader loader = Thread.currentThread().getContextClassLoader();           
		InputStream stream = loader.getResourceAsStream("integrationtestparameters.properties");
		prop.load(stream);
	
		// Save the props in the class attributes
		filesystemname 	= prop.getProperty("hadoop.it.filesystemname");
		username 		= prop.getProperty("hadoop.it.username");
		usergroup 		= prop.getProperty("hadoop.it.usergroup");
		filename 		= prop.getProperty("hadoop.it.filename");
		dir 			= prop.getProperty("hadoop.it.dir");
		permission 		= prop.getProperty("hadoop.it.permission");
		encoding 		= prop.getProperty("hadoop.it.encoding");
		messagepart1	= prop.getProperty("hadoop.it.messagepart1");
		messagepart2	= prop.getProperty("hadoop.it.messagepart2");
		
		conn = new HdfsConnector();
		conn.setDefaultFileSystemName(filesystemname);		
		conn.connect(username);
		
		// Clean up the directories before start the real test
		try {
			logger.debug(String.format("BEFORE - Delete if exists file /%s/%s", dir, filename));
			conn.deleteFile(String.format("/%s/%s", dir, filename));
		} catch (Throwable e) {}
		
		try {
			logger.debug(String.format("BEFORE - Delete if exists directory /%s", dir));
			conn.deleteDirectory(String.format("/%s", dir));
		} catch (Throwable e) {}
    }
    
    @Test
    public void hadoopConnectorTestIT() throws Exception {
    	assertDirNotExists();
    	assertFileNotExists();
    	createDir();
    	assertDirExists();
    	createFile();
    	assertFileExists();
    	Assert.assertEquals(retrieveFromFile(), messagepart1);
    	/**
		 * IMPORTANT:
		 * to be able append info in HDFS, you must set the property dfs.support.append in conf/hdfs-site.xml to true.
		 * Otherwise the service will throw an Exception
		 */
    	appendToFile();
    	Assert.assertEquals(retrieveFromFile(), messagepart1+messagepart2);
		deleteFile();
		assertFileNotExists();
		deleteDir();
		assertFileNotExists();
    }
    
    
    private void createDir() throws Exception {
    	// **** MAKE A NEW DIR ****
    	conn.makeDirectories(String.format("/%s", dir), permission);
    }
    
    private void createFile() throws Exception {
    	// **** CREATE A NEW FILE (OVERWRITE IF EXISTS) ****
		StringBuffer StringBuffer1 = new StringBuffer(messagepart1);
		InputStream inputStream = new ByteArrayInputStream(StringBuffer1.toString().getBytes(encoding));
		
		conn.writeToPath(String.format("/%s/%s", dir, filename), permission, true, 4096, 1, 4096, username, usergroup, inputStream);
    }
    
    private void appendToFile() throws Exception {
    	// **** APPEND DATA TO THE FILE ****
    	StringBuffer StringBuffer1 = new StringBuffer(messagepart2);
    	InputStream inputStream = new ByteArrayInputStream(StringBuffer1.toString().getBytes(encoding));
		
		conn.appendToPath(String.format("/%s/%s", dir, filename), 4096, inputStream);
    }
    
    private String retrieveFromFile() throws Exception {
    	// **** RETRIEVE DATA FROM FILE ****
		MySourceCallback sc = new MySourceCallback();
		
		return (String) conn.readFromPath(String.format("/%s/%s", dir, filename), 4096, sc);
    }
    
    private void deleteFile() throws Exception {
    	// **** DELETE FILE ****
    	conn.deleteFile(String.format("/%s/%s", dir, filename));
    }
    
    private void deleteDir() throws Exception {
    	// **** DELETE PATH ****
    	conn.deleteDirectory(String.format("/%s", dir));
    }
    
    private void assertFileExists() throws Exception {
		MyMuleEvent me = new MyMuleEvent();
		conn.getPathMetaData(String.format("/%s/%s", dir, filename), me);
		
		String result = me.getFlowVariable("hdfs.path.exists").toString();
		
		Assert.assertEquals(result, "true");
    }
    
    private void assertFileNotExists() throws Exception {
    	MyMuleEvent me = new MyMuleEvent();
		conn.getPathMetaData(String.format("/%s/%s", dir, filename), me);
		
		String result = me.getFlowVariable("hdfs.path.exists").toString();
		
		Assert.assertEquals(result, "false");
    }
    
    private void assertDirExists() throws Exception {
    	MyMuleEvent me = new MyMuleEvent();
		conn.getPathMetaData(String.format("/%s", dir), me);
		
		String result = me.getFlowVariable("hdfs.path.exists").toString();
		
		Assert.assertEquals(result, "true");
    }
    
    private void assertDirNotExists() throws Exception {
    	MyMuleEvent me = new MyMuleEvent();
		conn.getPathMetaData(String.format("/%s", dir), me);
		
		String result = me.getFlowVariable("hdfs.path.exists").toString();
		
		Assert.assertEquals(result, "false");
    }  
    
}

