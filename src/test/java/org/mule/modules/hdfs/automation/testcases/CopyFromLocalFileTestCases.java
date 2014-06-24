package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CopyFromLocalFileTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("copyFromLocalFileTestData");
        upsertOnTestRunMessage("path", getTestRunMessageValue("target"));
    }

    @Category({RegressionTests.class})
    @Test
    public void testCopyFromLocalFile() {
        try {
            runFlowAndGetPayload("copy-from-local-file");

            FileStatus[] fileStatuses = runFlowAndGetPayload("list-status");
            assertTrue(fileStatuses.length != 0);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
