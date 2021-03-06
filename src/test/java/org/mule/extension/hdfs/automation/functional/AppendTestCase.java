/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;

public class AppendTestCase extends BaseTest {

    public static final String MYFILE_PATH = "myfile.txt";
    private byte[] initialWrittenData;
    private final String PARENT_DIRECTORY = "hdfs-test-append/";

    public AppendTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return TestConstants.APPEND_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        initialWrittenData = TestDataBuilder.payloadShortString();

        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("permission", "700")
                .run();
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(initialWrittenData))
                .run();
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }

    @Test
    public void testAppend() throws Exception {
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        inputStreams.add(new ByteArrayInputStream(initialWrittenData));

        byte[] inputStreamToAppend = TestDataBuilder.payloadShortString();
        inputStreams.add(new ByteArrayInputStream(inputStreamToAppend));

        SequenceInputStream inputStreamsSequence = new SequenceInputStream(inputStreams.elements());

        flowRunner(TestConstants.FlowNames.APPEND_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(inputStreamToAppend))
                .run();

        CursorStream cursor = ((CursorStreamProvider) flowRunner(TestConstants.FlowNames.READ_OP_FLOW).withVariable("path", PARENT_DIRECTORY + MYFILE_PATH)
                .keepStreamsOpen()
                .run()
                .getMessage()
                .getPayload()
                .getValue()).openCursor();

        assertThat(IOUtils.contentEquals(inputStreamsSequence, (InputStream) cursor), is(true));
    }
}
