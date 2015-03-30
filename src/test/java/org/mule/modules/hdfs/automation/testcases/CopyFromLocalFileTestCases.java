/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

@Ignore("Fails on Amazon EC2, run this test on local Hadoop instance")
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

            List<FileStatus> fileStatuses = runFlowAndGetPayload("list-status");
            long initialLength = (new File((String) getTestRunMessageValue("source"))).length();
            assertTrue(fileStatuses.size() > 0);
            assertEquals(fileStatuses.get(0).getLen(), initialLength);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
