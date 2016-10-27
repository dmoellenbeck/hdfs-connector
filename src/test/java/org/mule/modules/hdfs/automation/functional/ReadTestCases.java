/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class ReadTestCases extends AbstractTestCases {

    private static final String MYFILE_PATH = "myfile.txt";
    public static final String READ_SOURCE = "read";
    private byte[] writtenData;

    @Before
    public void setUp() throws Throwable {
        createFileToMonitor();
        startMonitoringFile();
    }

    private void startMonitoringFile() throws Throwable {
        Object[] sourceSignature = new Object[] {
                MYFILE_PATH,
                4096,
                null
        };
        getDispatcher().initializeSource(READ_SOURCE, sourceSignature);
    }

    private void createFileToMonitor() throws Exception {
        writtenData = TestDataBuilder.payloadForRead();
        getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
    }

    @After
    public void tearDown() throws Throwable {
        try {
            getDispatcher().shutDownSource(READ_SOURCE);
        } finally {
            getConnector().deleteFile(MYFILE_PATH);
        }
    }

    @Test
    public void testRead() throws Exception {
        waitAWhileForSourceToCollectSomeMesages();
        List<Object> readContents = getDispatcher().getSourceMessages(READ_SOURCE);
        validateReadContents(readContents);
    }

    private void waitAWhileForSourceToCollectSomeMesages() throws InterruptedException {
        Thread.sleep(500);
    }

    private void validateReadContents(List<Object> readContents) throws Exception {
        assertThat(readContents, hasSize(greaterThan(0)));
        for (Object content : readContents) {
            InputStream actualContentStream = (InputStream) content;
            byte[] actualContent = new byte[actualContentStream.available()];
            IOUtils.read(actualContentStream, actualContent);
            assertThat("Read content is different from what was written.", actualContent, equalTo(writtenData));
        }
    }
}
