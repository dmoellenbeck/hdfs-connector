/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.construct.Flow;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("Fails on Amazon EC2, run this test on local Hadoop instance")
public class ReadTestCases extends HDFSTestParent {

    String fileContentString;

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("readTestData");
        InputStream fileContent = getTestRunMessageValue("payloadRef");
        fileContentString = IOUtils.toString(fileContent);
        upsertOnTestRunMessage("payloadRef", IOUtils.toInputStream(fileContentString));
        runFlowAndGetPayload("write-default-values");
    }

    @Category({RegressionTests.class})
    @Test
    public void testRead() {
        try {
            Flow flow = muleContext.getRegistry().get("read");
            flow.start();
            Object payload = muleContext.getClient().request("vm://receive", 5000).getPayload();
            assertEquals(fileContentString, payload);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-file");
    }
}
