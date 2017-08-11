package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;
import org.mule.runtime.core.exception.MessagingException;

public class WriteTestCases extends BaseTest {

    private final byte[] writtenData = RandomStringUtils.randomAlphanumeric(20)
            .getBytes();
    private final String FILE_PATH = "hdfs-test-write-file.txt";
    private final String NO_PERMISSION_FILE_PATH = "no-permission-write-file.txt";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return Util.WRITE_OPERATION_FLOW_PATH;
    }

    @Test
    public void testWriteFlow() throws Exception {

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
    public void testWriteNullPayloadFlow() throws Exception {
        expectedException.expect(MessagingException.class);
        expectedException.expectMessage(StringContains.containsString("Payload cannot be null"));

        InputStream payload = null;
        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH)
                .withPayload(payload)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    @Test
    public void testWriteNoPermissionFlow() throws Exception {

        InputStream payload = new ByteArrayInputStream(writtenData);
        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", NO_PERMISSION_FILE_PATH)
                .withVariable("permission", "000")
                .withPayload(payload)
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        List<FileStatus> listStatus = (List<FileStatus>) flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", NO_PERMISSION_FILE_PATH)
                .withPayload(payload)
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        assertThat("It should be at list one file.", listStatus.size() > 0);
        assertThat("File should have no permission.", listStatus.get(0)
                .getPermission()
                .toString(), equalTo("---------"));
    }
}
