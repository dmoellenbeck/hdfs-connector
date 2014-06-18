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

public class RenameTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("renameTestData");

        upsertOnTestRunMessage("path", getTestRunMessageValue("fromPath"));
        runFlowAndGetPayload("make-directories");
    }

    @Category({SmokeTests.class, RegressionTests.class})
    @Test
    public void testRename() {
        try {
            Boolean status = (Boolean) runFlowAndGetPayload("rename");
            assertTrue(status);
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
