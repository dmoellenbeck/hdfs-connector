/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@SuppressWarnings("unchecked")
public class GlobStatusTestCase extends BaseTest {

    private final String PARENT_DIRECTORY = "hdfs-test-glob-status/";
    public static final String INVALID_PATH_PATTERN = "invalidPathPattern";

    @Override
    protected String getConfigFile() {
        return TestConstants.GLOBAL_STATUS_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("permission", "700")
                .run();
        for (String fileName : TestDataBuilder.fileNamesForGlobStatus()) {
            flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + fileName)
                    .withPayload(TestDataBuilder.payloadForWrite())
                    .run();
        }
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }

    @Test
    public void testGlobStatus() throws Exception {
        String filter = new String("^.*2013/12/31$");
        List<FileStatus> fileStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.GLOB_STATUS_FLOW).withVariable("pathPattern", PARENT_DIRECTORY + "/2013/*/*")
                        .withVariable("filter", filter)
                        .run());

        assertThat(fileStatuses, notNullValue());
        assertThat(fileStatuses.size(), is(1));
        assertThat(fileStatuses.get(0)
                .getPath()
                .toString(), containsString("2013/12/31"));
    }

    @Test
    public void testGlobStatusWhenFilterSetToNull() throws Exception {

        List<FileStatus> fileStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.GLOB_STATUS_FLOW).withVariable("pathPattern", PARENT_DIRECTORY + "/2013/*/*")
                        .withVariable("filter", null)
                        .run());

        assertThat(fileStatuses, notNullValue());
        assertThat(fileStatuses.size(), is(2));
        assertThat(fileStatuses.get(0)
                .getPath()
                .toString(), containsString("2013/12/30"));
    }

    @Test
    public void testGlobStatusWhenNoFileMatches() throws Exception {

        List<FileStatus> fileStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.GLOB_STATUS_FLOW).withVariable("pathPattern", PARENT_DIRECTORY + INVALID_PATH_PATTERN)
                        .run());

        assertThat(fileStatuses, notNullValue());
        assertThat(fileStatuses, empty());
    }

}
