///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.apache.commons.io.IOUtils;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.io.InputStream;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//
//import static org.hamcrest.Matchers.is;
//
//public class CopyFromLocalFileTestCases extends AbstractTestCases {
//
//    public static final String TARGET_DIRECTORY = "rootDirectory/";
//    public static final String LOCAL_SOURCE_PATH = "src/test/resources/data-sets/timeZones.txt";
//
//    @Test
//    public void testCopyFromLocalFile() throws Exception {
//        Path localSource = Paths.get(LOCAL_SOURCE_PATH);
//        Path remoteTarget = Paths.get(TARGET_DIRECTORY)
//                .resolve(localSource.getFileName());
//        getConnector().copyFromLocalFile(false, true, LOCAL_SOURCE_PATH, remoteTarget.toString());
//        InputStream targetDataStream = getConnector().readOperation(remoteTarget.toString(), 4096);
//        InputStream sourceDataStream = Files.newInputStream(localSource);
//        Assert.assertThat(IOUtils.contentEquals(targetDataStream, sourceDataStream), is(true));
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        getConnector().deleteDirectory(TARGET_DIRECTORY);
//    }
//}
