/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is
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
import org.mule.construct.Flow;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Vector;

import static org.junit.Assert.fail;

@Ignore("Fails on Amazon EC2, run this test on local Hadoop instance")
public class AppendTestCases extends HDFSTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("appendTestData");
        runFlowAndGetPayload("write-default-values");
    }

    @Category({RegressionTests.class})
    @Test
    public void testAppend() {

        Vector<InputStream> inputStreams = new Vector<InputStream>();
        inputStreams.add((InputStream) getTestRunMessageValue("payloadRef"));

        InputStream inputStreamToAppend = getBeanFromContext("randomInputStream");
        inputStreams.add(inputStreamToAppend);
        upsertOnTestRunMessage("payloadRef", inputStreamToAppend);

        SequenceInputStream inputStreamsSequence = new SequenceInputStream(inputStreams.elements());

        try {
            runFlowAndGetPayload("append");
            Flow flow = muleContext.getRegistry().get("read");
            flow.start();
            String payload = (String) muleContext.getClient().request("vm://receive", 5000).getPayload();
            IOUtils.contentEquals(inputStreamsSequence, IOUtils.toInputStream(payload));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-file");
    }
}
