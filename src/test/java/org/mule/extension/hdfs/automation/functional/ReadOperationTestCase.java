/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;
import org.mule.runtime.core.api.exception.MessagingException;


public class ReadOperationTestCase extends BaseTest {

    private final byte[] writtenData = RandomStringUtils.randomAlphanumeric(20)
            .getBytes();
    private final String FILE_PATH = "hdfs-test-file.txt";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return TestConstants.READ_OPERATION_FLOW_PATH;
    }

    @Test
    public void testReadFlow() throws Exception {

        InputStream payload = new ByteArrayInputStream(writtenData);
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH)
                .withPayload(payload)
                .run();

        CursorStream cursor = ((CursorStreamProvider) flowRunner(TestConstants.FlowNames.READ_OP_FLOW).withVariable("path", FILE_PATH)
                .keepStreamsOpen()
                .run()
                .getMessage()
                .getPayload()
                .getValue())
                .openCursor();

        byte[] actualContent = IOUtils.toByteArray(cursor);
        assertThat("Read content is different from what was written.", actualContent, equalTo(writtenData));

    }

    @Test
    public void testReadOpFlowUnexistingFile() throws Exception {

        expectedException.expect(MessagingException.class);
        expectedException.expectMessage(StringContains.containsString(TestConstants.ExceptionMessages.INVALID_REQUEST_DATA));

        flowRunner(TestConstants.FlowNames.READ_OP_FLOW).withVariable("path", "unexisting.txt")
                .run();
    }

}
