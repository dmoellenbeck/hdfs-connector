/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
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
        initializeTestRunMessage("copyFromLocalFileTestData");
        upsertOnTestRunMessage("source", "src/test/resources/data-sets/");
        runFlowAndGetPayload("copy-from-local-file");
        source = getTestRunMessageValue("source");
        target = getTestRunMessageValue("target");
        initializeTestRunMessage("copyToLocalFileTestData");
    }

    @Test
    @Category({RegressionTests.class})
    public void testCopyToLocalFile() {
        try {
            runFlowAndGetPayload("copy-to-local-file");
            File sourceFile = new File(source+"/timeZones.txt");
            File targetFile = new File((String) getTestRunMessageValue("targetFile"));
            FileUtils.contentEquals(sourceFile, targetFile);
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File((String) getTestRunMessageValue("target")));
    }
}