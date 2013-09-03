package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.api.MuleEvent;
import org.mule.api.processor.MessageProcessor;

public class AppendTestCases extends HDFSTestParent {

	@Before
	public void setUp() {
		try {
			testObjects = (Map<String, Object>) context.getBean("append");
			
			String path = (String) testObjects.get("path");
			String fileContent = (String) testObjects.get("payloadRef");
			
			write(path, fileContent);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Category({SmokeTests.class})
	@Test
	@Ignore
	public void testAppend() {
		try {
			String fileContent = (String) testObjects.get("payloadRef");
			String toAppend = (String) testObjects.get("toAppend");
			
			testObjects.put("payloadRef", toAppend);
			
			MessageProcessor flow = lookupFlowConstruct("append");
			MuleEvent response = flow.process(getTestEvent(testObjects));

			InputStream obj = (InputStream) response.getMessage().getPayload();
			String content = IOUtils.toString(obj);
			
			assertTrue(content.equals(fileContent + toAppend));
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
