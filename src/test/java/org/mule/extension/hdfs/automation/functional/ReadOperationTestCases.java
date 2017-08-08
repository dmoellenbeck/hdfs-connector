package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;
import org.mule.runtime.core.exception.MessagingException;

public class ReadOperationTestCases extends BaseTest {

    private final byte[] writtenData = RandomStringUtils.randomAlphanumeric(20)
            .getBytes();
    private final String FILE_PATH = "hdfs-test-file.txt";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return Util.READ_OPERATION_FLOW_PATH;
    }

    @Test
    public void testReadFlow() throws Exception {

        InputStream payload = new ByteArrayInputStream(writtenData);
        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH)
                .withPayload(payload)
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        CursorStream cursor = ((CursorStreamProvider) flowRunner(Util.FlowNames.READ_OP_FLOW).withVariable("path", FILE_PATH)
                .keepStreamsOpen()
                .run()
                .getMessage()
                .getPayload()
                .getValue()).openCursor();

        byte[] actualContent = IOUtils.toByteArray(cursor);
        assertThat("Read content is different from what was written.", actualContent, equalTo(writtenData));

    }

    @Test
    public void testReadOpFlowUnexistingFile() throws Exception {

        expectedException.expect(MessagingException.class);
        expectedException.expectMessage(StringContains.containsString("Something went wrong with input data"));

        flowRunner(Util.FlowNames.READ_OP_FLOW).withVariable("path", "unexisting.txt")
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

}
