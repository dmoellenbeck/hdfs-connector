///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.ExpectedException;
//
//public class DeleteDirectoryTestCases extends AbstractTestCases {
//
//    private static final String PARENT_DIRECTORY = "rootDirectory/";
//    private static final String NEW_DIRECTORY = "newDirectory";
//    @Rule
//    public ExpectedException fileNotFoundExpected = ExpectedException.none();
//
//    @Before
//    public void setUp() throws Exception {
//        getConnector().makeDirectories(PARENT_DIRECTORY + NEW_DIRECTORY, "700");
//    }
//
//    @Test
//    public void testDeleteDirectory() throws Exception {
//        getConnector().deleteDirectory(PARENT_DIRECTORY + NEW_DIRECTORY);
//        verifyDeletetionOfDirectory();
//    }
//
//    private void verifyDeletetionOfDirectory() throws Exception {
//        fileNotFoundExpected.expect(Exception.class);
//        getConnector().listStatus(PARENT_DIRECTORY + NEW_DIRECTORY, null);
//    }
//
//}
