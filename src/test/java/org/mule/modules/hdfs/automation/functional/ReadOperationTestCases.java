/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.modules.hdfs.exception.HDFSConnectorException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ReadOperationTestCases extends AbstractTestCases {

    private static final String MYFILE_PATH = "myfile.txt";
    private static final String FAKE_PATH = "fakefile.txt";
    private byte[] writtenData;
    @Rule
    public ExpectedException fileNotFoundException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadForRead();
        getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
    }

    @After
    public void tearDown() throws Exception {
        getConnector().deleteFile(MYFILE_PATH);
    }

    @Test
    public void testReadOperation() throws Exception {
        InputStream actualContentInpuStream = getConnector().readOperation(MYFILE_PATH, 4096);
        byte[] actualContent = new byte[actualContentInpuStream.available()];
        IOUtils.read(actualContentInpuStream, actualContent);
        assertThat("Read content is different from what was written.", actualContent, equalTo(writtenData));
    }

    @Test
    public void testReadOperationWhenFileDoesNotExist() throws Exception {
        fileNotFoundException.expect(HDFSConnectorException.class);
        getConnector().readOperation(FAKE_PATH, 4096);
    }

}
