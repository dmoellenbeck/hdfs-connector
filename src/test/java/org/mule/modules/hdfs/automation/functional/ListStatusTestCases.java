/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class ListStatusTestCases extends AbstractTestCases {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("listStatusTestData");
        int itr = Integer.parseInt(getTestRunMessageValue("size").toString());
        String root = getTestRunMessageValue("path");
        for (int i = 0; i < itr; i++) {
            upsertOnTestRunMessage("path", root + "/" + UUID.randomUUID()
                    .toString() + ".txt");
            runFlowAndGetPayload("write-default-values");
        }
        root = (((String) getTestRunMessageValue("path")).split("/"))[0];
        upsertOnTestRunMessage("path", root);
    }

    @Test
    public void testListStatus() {
        try {

            List<FileStatus> fileStatuses = runFlowAndGetPayload("list-status");
            assertNotNull(fileStatuses);
            assertEquals((Integer.parseInt((String) getTestRunMessageValue("size"))), fileStatuses.size());
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
