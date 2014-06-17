/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Vector;

import static org.junit.Assert.fail;

@Ignore
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

        SequenceInputStream inputStreamsSequence = new SequenceInputStream((Enumeration<InputStream>) inputStreams.elements());

        try {
            runFlowAndGetPayload("append");
            IOUtils.contentEquals(inputStreamsSequence, (InputStream) runFlowAndGetPayload("read"));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }

    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-file");
    }
}
