package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({RegressionTests.class})
public class SetPermissionTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("setPermissionTestData");
        runFlowAndGetPayload("make-directories");
    }

    @Test
    public void testSetPermission() {
        try {
            FsPermission oldPermission = new FsPermission((String) getTestRunMessageValue("permission"));
            runFlowAndGetPayload("set-permission");
            upsertOnTestRunMessage("path", getTestRunMessageValue("rootPath"));

            FileStatus[] fileStatuses = runFlowAndGetPayload("list-status");
            FsPermission newPermission = fileStatuses[0].getPermission();
            assertTrue(newPermission.equals(oldPermission));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}