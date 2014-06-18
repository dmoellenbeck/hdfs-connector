package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.SmokeTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ListStatusTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("listStatusTestData");
        int itr = Integer.parseInt(getTestRunMessageValue("size").toString());
        String root = getTestRunMessageValue("path");
        for (int i = 0; i <= itr; i++) {
            upsertOnTestRunMessage("path", root + "/" + UUID.randomUUID().toString() + ".txt");
            runFlowAndGetPayload("write-default-values");
        }
        root = (((String) getTestRunMessageValue("path")).split("/"))[0];
        upsertOnTestRunMessage("path", root);
    }

    @Category({SmokeTests.class, RegressionTests.class})
    @Test
    public void testListStatus() {
        try {

            FileStatus[] fileStatuses = runFlowAndGetPayload("list-status");
            assertTrue(fileStatuses != null);
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
