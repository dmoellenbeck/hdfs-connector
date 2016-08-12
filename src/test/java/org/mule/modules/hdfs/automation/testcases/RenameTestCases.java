/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.testcases;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RenameTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("renameTestData");
    }

    @Category({ RegressionTests.class
    })
    @Test
    public void testRenameDir() {
        try {
            upsertOnTestRunMessage("path", getTestRunMessageValue("fromPath"));
            runFlowAndGetPayload("make-directories");

            assertTrue((Boolean) runFlowAndGetPayload("rename"));
            upsertOnTestRunMessage("path", getTestRunMessageValue("toPath"));
            assertTrue((Boolean) runFlowAndGetInvocationProperty("get-metadata", HDFSConnector.HDFS_PATH_EXISTS));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @Ignore("Fails on Amazon EC2, run this test on local Hadoop instance")
    @Category({ RegressionTests.class
    })
    @Test
    public void testRenameFile() {
        try {
            upsertOnTestRunMessage("path", getTestRunMessageValue("fromPath"));
            runFlowAndGetPayload("write");

            assertTrue((Boolean) runFlowAndGetPayload("rename"));
            upsertOnTestRunMessage("path", getTestRunMessageValue("toPath"));
            assertTrue((Boolean) runFlowAndGetInvocationProperty("get-metadata", HDFSConnector.HDFS_PATH_EXISTS));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
