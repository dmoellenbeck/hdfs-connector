/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;

public class CopyFromLocalFileTestCase extends BaseTest {

    public static final String TARGET_DIRECTORY = "rootDirectory/";
    public static final String LOCAL_SOURCE_PATH = "src/test/resources/data-sets/timeZones.txt";

    public CopyFromLocalFileTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return TestConstants.COPY_FROM_LOCAL_FLOW_PATH;
    }

    @Test
    public void testCopyFromLocalFile() throws Exception {
        Path localSource = Paths.get(LOCAL_SOURCE_PATH);
        Path remoteTarget = Paths.get(TARGET_DIRECTORY)
                .resolve(localSource.getFileName());

        flowRunner(TestConstants.FlowNames.COPY_FROM_LOCAL_FILE_FLOW).withVariable("deleteSource", false)
                .withVariable("overwrite", true)
                .withVariable("source", LOCAL_SOURCE_PATH)
                .withVariable("destination", remoteTarget.toString())
                .run();

        CursorStream cursor = ((CursorStreamProvider) flowRunner(TestConstants.FlowNames.READ_OP_FLOW).withVariable("path", remoteTarget.toString())
                .keepStreamsOpen()
                .run()
                .getMessage()
                .getPayload()
                .getValue()).openCursor();

        InputStream sourceDataStream = Files.newInputStream(localSource);
        assertThat(IOUtils.contentEquals(cursor, sourceDataStream), is(true));
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", TARGET_DIRECTORY)
                .run();
    }
}
