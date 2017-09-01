package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;

public class CopyToLocalFileTestCases extends BaseTest {

    private static final String MYFILE_PATH = "myfile.txt";
    private static final String LOCAL_TARGET_PATH = "src/test/resources/data-sets/myfile.txt";
    private byte[] initialWrittenData;

    @Override
    protected String getConfigFile() {
        return Util.COPY_TO_LOCAL_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        initialWrittenData = TestDataBuilder.payloadShortString();
        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(initialWrittenData))
                .run();
    }

    @Test
    public void testCopyToLocalFile() throws Exception {
        flowRunner(Util.FlowNames.COPY_TO_LOCAL_FILE_FLOW).withVariable("deleteSource", false)
                .withVariable("useRawLocalFileSystem", false)
                .withVariable("source", MYFILE_PATH)
                .withVariable("destination", LOCAL_TARGET_PATH)
                .run();

        Path localTarget = Paths.get(LOCAL_TARGET_PATH);
        InputStream targetDataStream = Files.newInputStream(localTarget);
        InputStream sourceDataStream = new ByteArrayInputStream(initialWrittenData);
        assertThat(IOUtils.contentEquals(targetDataStream, sourceDataStream), is(true));
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(Util.FlowNames.DELETE_FILE_FLOW).withVariable("path", MYFILE_PATH)
                .run();
        Files.delete(Paths.get(LOCAL_TARGET_PATH));
        Path localTarget = Paths.get(LOCAL_TARGET_PATH);
        Files.delete(localTarget.resolveSibling("." + localTarget.getFileName()
                .toString() + ".crc"));
    }
}
