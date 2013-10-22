/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

public class ReadTestCases extends HDFSTestParent {

	@Before
	public void setUp() throws Exception {
		initializeTestRunMessage("readTestData");
		runFlowAndGetPayload("write-default-values");

	}
	
	@Ignore
	@Category({RegressionTests.class})
	@Test
	public void testRead() {
		try {
			String fileContent = getTestRunMessageValue("payloadRef");
			InputStream obj = runFlowAndGetPayload("read");
			String content = IOUtils.toString(obj);
			assertTrue(content.equals(fileContent));
			
		} catch (Exception e) {
			fail(ConnectorTestUtils.getStackTrace(e));
		}

	}

	@After
	public void tearDown() throws Exception {
		runFlowAndGetPayload("delete-file");
	}
}
