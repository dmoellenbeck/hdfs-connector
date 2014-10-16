/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;

import static org.junit.Assert.*;

public class GlobStatusTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("globStatusTestData");
        for (String path : (List<String>) getTestRunMessageValue("pathList")) {
            upsertOnTestRunMessage("path", path);
            runFlowAndGetPayload("make-directories");
        }
    }

    @Category({RegressionTests.class})
    @Test
    public void testGlobStatus() {
        try {
            FileStatus[] fileStatuses = runFlowAndGetPayload("glob-status");
            assertNotNull(fileStatuses);
            assertTrue((fileStatuses[0].getPath().toString()).contains(getTestRunMessageValue("result").toString()));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        upsertOnTestRunMessage("path", ".");
        runFlowAndGetPayload("delete-directory");
    }
}
