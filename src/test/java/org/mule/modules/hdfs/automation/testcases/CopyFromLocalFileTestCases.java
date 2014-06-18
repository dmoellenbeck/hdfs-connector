package org.mule.modules.hdfs.automation.testcases;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.SmokeTests;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.fail;

public class CopyFromLocalFileTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("copyFromLocalFileTestData");
    }

    @Category({SmokeTests.class, RegressionTests.class})
    @Test
    public void testCopyFromLocalFile() {
        try {
            runFlowAndGetPayload("copy-from-local-file");

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {

    }
}
