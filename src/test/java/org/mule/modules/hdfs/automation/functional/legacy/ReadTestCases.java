/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional.legacy;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.construct.Flow;
import org.mule.modules.hdfs.exception.HDFSConnectorException;
import org.mule.modules.hdfs.utils.IsCausedBy;
import org.mule.modules.tests.ConnectorTestCase;
import org.mule.modules.tests.ConnectorTestUtils;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadTestCases extends ConnectorTestCase {

    String fileContentString;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("readTestData");
        InputStream fileContent = getTestRunMessageValue("payloadRef");
        fileContentString = IOUtils.toString(fileContent);
        upsertOnTestRunMessage("payloadRef", IOUtils.toInputStream(fileContentString));
        runFlowAndGetPayload("write-default-values");
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("delete-file");
    }

    @Test
    public void testRead() {
        try {
            Flow flow = muleContext.getRegistry()
                    .get("read");
            flow.start();
            Object payload = muleContext.getClient()
                    .request("vm://receive", 5000)
                    .getPayload();
            assertEquals(fileContentString, payload);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @Test
    public void testReadOperation() throws Exception {
        String payload = runFlowAndGetPayload("readOperation");
        assertEquals(fileContentString, payload);
    }

    @Test
    public void testReadOperationWhenFileDoesNotExist() throws Exception {
        expectedException.expect(new IsCausedBy(HDFSConnectorException.class, ""));
        runFlowAndGetPayload("readOperationWhenFileDoesNotExist");
    }

}
