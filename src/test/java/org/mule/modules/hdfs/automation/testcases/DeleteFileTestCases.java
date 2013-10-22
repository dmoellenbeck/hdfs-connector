/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.HdfsConnector;
import org.mule.modules.tests.ConnectorTestUtils;

public class DeleteFileTestCases extends HDFSTestParent {

	@Before
	public void setUp() throws Exception {
			initializeTestRunMessage("deleteTestData");
			runFlowAndGetPayload("write-default-values");

	}
	
	@Category({SmokeTests.class, RegressionTests.class})
	@Test
	public void testDeleteFile() {
		try {
			runFlowAndGetPayload("delete-file");
			assertFalse((Boolean) runFlowAndGetInvocationProperty("get-metadata", HdfsConnector.HDFS_PATH_EXISTS));
			
		} catch (Exception e) {
			fail(ConnectorTestUtils.getStackTrace(e));
		}

	}
	
}
