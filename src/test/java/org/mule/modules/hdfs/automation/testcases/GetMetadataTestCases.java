package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.api.MuleEvent;
import org.mule.api.processor.MessageProcessor;
import org.mule.modules.hdfs.HdfsConnector;


public class GetMetadataTestCases extends HDFSTestParent {

	@Before
	public void setUp() {
		try {
			testObjects = (Map<String, Object>) context.getBean("getMetadata");
			
			String path = (String) testObjects.get("path");
			String fileContent = (String) testObjects.get("payloadRef");
			
			write(path, fileContent);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Category({SmokeTests.class, RegressionTests.class})
	@Test
	public void testGetMetadata() {
		try {
			MessageProcessor flow = lookupFlowConstruct("get-metadata");
			MuleEvent response = flow.process(getTestEvent(testObjects));
			
			boolean exists =(Boolean) response.getMessage().getInvocationProperty(HdfsConnector.HDFS_PATH_EXISTS);
			assertTrue(exists);
			
			MD5MD5CRC32FileChecksum fileMd5 = response.getMessage().getInvocationProperty(HdfsConnector.HDFS_FILE_CHECKSUM);
			assertNotNull(fileMd5);
			
			FileStatus fileStatus = response.getMessage().getInvocationProperty(HdfsConnector.HDFS_FILE_STATUS);
			assertFalse(fileStatus.isDir());
			
			ContentSummary contentSummary = response.getMessage().getInvocationProperty(HdfsConnector.HDFS_CONTENT_SUMMARY);
			assertNotNull(contentSummary);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@After
	public void tearDown() {
		try {
			String path = (String) testObjects.get("path");
			deleteFile(path);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
		
}
