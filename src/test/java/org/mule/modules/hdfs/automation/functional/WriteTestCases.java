///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.apache.commons.io.IOUtils;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//
//import static org.hamcrest.Matchers.equalTo;
//import static org.junit.Assert.assertThat;
//
//public class WriteTestCases extends AbstractTestCases {
//
//    private static final String MYFILE_PATH = "myfile.txt";
//    private byte[] writtenData;
//
//    @Before
//    public void setUp() throws Exception {
//        writtenData = TestDataBuilder.payloadForWrite();
//    }
//
//    @Test
//    public void testWrite() throws Exception {
//        getConnector().write(MYFILE_PATH, "755", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
//        validateWrittenContent();
//    }
//
//    private void validateWrittenContent() throws Exception {
//        InputStream actualInputStream = getConnector().readOperation(MYFILE_PATH, 4096);
//        byte[] actualContent = new byte[actualInputStream.available()];
//        IOUtils.read(actualInputStream, actualContent);
//        assertThat("Read content is different from what was written.", actualContent, equalTo(writtenData));
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        getConnector().deleteFile(MYFILE_PATH);
//    }
//
//}
