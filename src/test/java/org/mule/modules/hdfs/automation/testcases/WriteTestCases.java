package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.api.MuleEvent;
import org.mule.api.processor.MessageProcessor;

public class WriteTestCases extends HDFSTestParent {

	@Before
	public void setUp() {
		try {
			testObjects = (Map<String, Object>) context.getBean("write");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Category({SmokeTests.class, RegressionTests.class})
	@Test
	public void testWrite() {
		try {
			MessageProcessor flow = lookupFlowConstruct("write");
			MuleEvent response = flow.process(getTestEvent(testObjects));
			
			String path = (String) testObjects.get("path");
			assertTrue(fileExists(path));
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
