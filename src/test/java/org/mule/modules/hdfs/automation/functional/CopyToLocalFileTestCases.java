///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.apache.commons.io.IOUtils;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//
//import static org.hamcrest.Matchers.is;
//
//public class CopyToLocalFileTestCases extends AbstractTestCases {
//
//    private static final String MYFILE_PATH = "myfile.txt";
//    private static final String LOCAL_TAGET_PATH = "src/test/resources/data-sets/myfile.txt";
//    private byte[] initialWrittenData;
//
//    @Before
//    public void setUp() throws Exception {
//        initialWrittenData = TestDataBuilder.payloadForCopyToLocal();
//        getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(initialWrittenData));
//    }
//
//    @Test
//    public void testCopyToLocalFile() throws Exception {
//        getConnector().copyToLocalFile(false, false, MYFILE_PATH, LOCAL_TAGET_PATH);
//        Path localTarget = Paths.get(LOCAL_TAGET_PATH);
//        InputStream targetDataStream = Files.newInputStream(localTarget);
//        InputStream sourceDataStream = new ByteArrayInputStream(initialWrittenData);
//        Assert.assertThat(IOUtils.contentEquals(targetDataStream, sourceDataStream), is(true));
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        getConnector().deleteFile(MYFILE_PATH);
//        Files.delete(Paths.get(LOCAL_TAGET_PATH));
//        Path localTarget = Paths.get(LOCAL_TAGET_PATH);
//        Files.delete(localTarget.resolveSibling("." + localTarget.getFileName()
//                .toString() + ".crc"));
//    }
//}