package org.mule.modules.hdfs.automation.testcases;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({RegressionTests.class})
public class CopyToLocalFileTestCases extends HDFSTestParent {
    String target;

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("copyToLocalFileTestData");
        runFlowAndGetPayload("copy-from-local-file");
        String source = getTestRunMessageValue("target");
        target = getTestRunMessageValue("source");
        upsertOnTestRunMessage("source", source);
        upsertOnTestRunMessage("target", target);
    }

    @Test
    public void testCopyToLocalFile() {
        try {
            runFlowAndGetPayload("copy-to-local-file");

            assertTrue(FileUtils.sizeOfDirectory(new File(target)) != 0);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }
}