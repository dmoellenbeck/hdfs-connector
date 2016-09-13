/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Vector;

import static org.hamcrest.Matchers.is;

public class AppendTestCases extends AbstractTestCases {

    public static final String MYFILE_PATH = "myfile.txt";
    private byte[] initialWrittenData;

    @Before
    public void setUp() throws Exception {
        initialWrittenData = TestDataBuilder.payloadForAppend();
        getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(initialWrittenData));
    }

    @After
    public void tearDown() throws Exception {
        getConnector().deleteFile(MYFILE_PATH);
    }

    @Test
    public void testAppend() throws Exception {
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        inputStreams.add(new ByteArrayInputStream(initialWrittenData));

        byte[] inputStreamToAppend = TestDataBuilder.payloadForAppend();
        inputStreams.add(new ByteArrayInputStream(inputStreamToAppend));

        SequenceInputStream inputStreamsSequence = new SequenceInputStream(inputStreams.elements());

        getConnector().append(MYFILE_PATH, 4096, new ByteArrayInputStream(inputStreamToAppend));
        InputStream payload = getConnector().readOperation(MYFILE_PATH, 4096);
        Assert.assertThat(IOUtils.contentEquals(inputStreamsSequence, payload), is(true));
    }
}
