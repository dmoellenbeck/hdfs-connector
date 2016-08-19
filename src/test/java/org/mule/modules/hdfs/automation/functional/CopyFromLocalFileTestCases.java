/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class CopyFromLocalFileTestCases extends AbstractTestCases {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("copyFromLocalFileTestData");
        upsertOnTestRunMessage("path", getTestRunMessageValue("target"));
    }

    @Test
    public void testCopyFromLocalFile() {
        try {
            runFlowAndGetPayload("copy-from-local-file");

            List<FileStatus> fileStatuses = runFlowAndGetPayload("list-status");
            long initialLength = (new File((String) getTestRunMessageValue("source"))).length();
            assertTrue(fileStatuses.size() > 0);
            assertEquals(fileStatuses.get(0)
                    .getLen(), initialLength);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-directory");
    }
}
