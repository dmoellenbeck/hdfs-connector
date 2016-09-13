/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional.legacy;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.tests.ConnectorTestCase;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SetPermissionTestCases extends ConnectorTestCase {

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

            List<FileStatus> fileStatuses = runFlowAndGetPayload("list-status");
            FsPermission newPermission = fileStatuses.get(0)
                    .getPermission();
            assertEquals(newPermission, oldPermission);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}