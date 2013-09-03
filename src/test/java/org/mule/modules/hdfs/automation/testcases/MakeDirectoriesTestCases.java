package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class MakeDirectoriesTestCases extends HDFSTestParent {

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		try {
			testObjects = (Map<String, Object>) context.getBean("makeDirectories");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Category({SmokeTests.class, RegressionTests.class})
	@Test
	public void testMakeDirectories() {
		try {
			String path = (String) testObjects.get("path");
			makeDirectories(path);
			
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
			String rootDir = path.substring(0, path.indexOf(File.separator, 1));
			
			deleteDirectory(rootDir);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
}
