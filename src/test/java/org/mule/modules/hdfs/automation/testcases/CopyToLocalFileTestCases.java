/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.testcases;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.File;

import static org.junit.Assert.fail;

@Ignore("Fails on Amazon EC2, run this test on local Hadoop instance")
public class CopyToLocalFileTestCases extends HDFSTestParent {

    String source, target;

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("copyToLocalFileTestData");
        runFlowAndGetPayload("copy-from-local-file");
        upsertOnTestRunMessage("source", getTestRunMessageValue("sourceConfig"));
        upsertOnTestRunMessage("target", getTestRunMessageValue("targetConfig"));
        upsertOnTestRunMessage("deleteSrc", "true");
    }

    @Test
    @Category({ RegressionTests.class
    })
    public void testCopyToLocalFile() {
        try {

            runFlowAndGetPayload("copy-to-local-file");
            File sourceFile = new File((String) getTestRunMessageValue("sourceFile"));
            File targetFile = new File((String) getTestRunMessageValue("targetFile"));
            FileUtils.contentEquals(sourceFile, targetFile);
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File((String) getTestRunMessageValue("target")));
    }
}