/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.SmokeTests;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MakeDirectoriesTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("makeDirectoriesTestData");
    }

    @Category({SmokeTests.class, RegressionTests.class})
    @Test
    public void testMakeDirectories() {
        try {
            runFlowAndGetPayload("make-directories");
            assertTrue((Boolean) runFlowAndGetInvocationProperty("get-metadata", HDFSConnector.HDFS_PATH_EXISTS));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        String root = (((String) getTestRunMessageValue("path")).split("/"))[0];
        upsertOnTestRunMessage("path", root);
        runFlowAndGetPayload("delete-directory");
    }

}
