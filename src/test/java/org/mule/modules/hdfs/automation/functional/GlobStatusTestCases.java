///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.functional;
//
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.PathFilter;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.mule.modules.hdfs.utils.RegexExcludePathFilter;
//
//import java.util.List;
//
//import static org.hamcrest.Matchers.*;
//
//public class GlobStatusTestCases extends AbstractTestCases {
//
//    public static final String PARENT_DIRECTORY = "rootDirectory/";
//    public static final String INVALID_PATH_PATTERN = "invalidPathPattern";
//
//    @Before
//    public void setUp() throws Exception {
//        for (String childFile : TestDataBuilder.fileNamesForGlobStatus()) {
//            getConnector().makeDirectories(PARENT_DIRECTORY + childFile, "700");
//        }
//    }
//
//    @Test
//    public void testGlobStatus() throws Exception {
//        List<FileStatus> fileStatuses = getConnector().globStatus(PARENT_DIRECTORY + "/2013/*/*", new RegexExcludePathFilter("^.*2013/12/31$"));
//        Assert.assertThat(fileStatuses, notNullValue());
//        Assert.assertThat(fileStatuses.get(0)
//                .getPath()
//                .toString(), containsString("2013/12/30"));
//    }
//
//    @Test
//    public void testGlobStatusWhenNoFileMatches() throws Exception {
//        List<FileStatus> fileStatuses = getConnector().globStatus(PARENT_DIRECTORY + "/2013/*/*", new PathFilter() {
//
//            @Override
//            public boolean accept(Path path) {
//                return false;
//            }
//        });
//        Assert.assertThat(fileStatuses, notNullValue());
//        Assert.assertThat(fileStatuses, empty());
//    }
//
//    @Test
//    public void testGlobStatusWhenFilterSetToNull() throws Exception {
//        List<FileStatus> fileStatuses = getConnector().globStatus(PARENT_DIRECTORY + "/2013/*/*", null);
//        Assert.assertThat(fileStatuses, notNullValue());
//        Assert.assertThat(fileStatuses.get(0)
//                .getPath()
//                .toString(), containsString("2013/12/30"));
//    }
//
//    @Test
//    public void testGlobStatusWhenNoFile() throws Exception {
//        List<FileStatus> fileStatuses = getConnector().globStatus(PARENT_DIRECTORY + INVALID_PATH_PATTERN, new RegexExcludePathFilter("^.*2013/12/31$"));
//        Assert.assertThat(fileStatuses, notNullValue());
//        Assert.assertThat(fileStatuses, empty());
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        getConnector().deleteDirectory(PARENT_DIRECTORY);
//    }
//}
