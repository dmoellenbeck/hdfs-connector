///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.mule.modules.tests.ConnectorTestUtils;
//
//import java.util.Arrays;
//import java.util.List;
//
///**
// * @author MuleSoft, Inc.
// */
//public class TestDataBuilder {
//
//    public static byte[] payloadForAppend() throws Exception {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static List<String> fileNamesForListStatus() {
//        return Arrays.asList("file1.txt", "file2.txt", "file3.txt");
//    }
//
//    public static List<String> fileNamesForGlobStatus() {
//        return Arrays.asList("2013/12/30", "2013/12/31", "2014/03/20", "2014/04/22");
//    }
//
//    public static byte[] payloadForCopyToLocal() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForDelete() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForListStatus() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForWrite() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForGetMetadata() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForRead() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForReadOperation() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForRename() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForSetPermissions() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//
//    public static byte[] payloadForSetOwner() {
//        return ConnectorTestUtils.generateRandomShortString()
//                .getBytes();
//    }
//}
