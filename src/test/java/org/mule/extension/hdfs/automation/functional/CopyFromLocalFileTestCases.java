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
import org.mule.modules.hdfs.automation.functional.BaseTest;
import org.mule.runtime.api.streaming.bytes.CursorStream;
import org.mule.runtime.api.streaming.bytes.CursorStreamProvider;

public class CopyFromLocalFileTestCases extends BaseTest {

    public static final String TARGET_DIRECTORY = "rootDirectory/";
    public static final String LOCAL_SOURCE_PATH = "src/test/resources/data-sets/timeZones.txt";

    @Override
    protected String getConfigFile() {
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
