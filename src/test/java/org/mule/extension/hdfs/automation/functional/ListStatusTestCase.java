/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.runtime.core.api.event.CoreEvent;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@SuppressWarnings("unchecked")
public class ListStatusTestCase extends BaseTest {

    private final String PARENT_DIRECTORY = "hdfs-test-list-status/";
    private final String FILE_PATH1 = PARENT_DIRECTORY + "hdfs-test-write-file1.txt";
    private final String FILE_PATH2 = PARENT_DIRECTORY + "hdfs-test-write-file2.txt";

    private final String OWNER = "hdfs-connector";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return TestConstants.LIST_STATUS_FLOW_PATH;
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
    public void testStatusPermissionFlow() throws Exception {

        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH1)
                .withVariable("permission", "000")
                .withPayload(TestDataBuilder.payloadForWrite())
                .run();

        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", FILE_PATH1)
                        .run());

        assertThat("It should be at list one file.", listStatus.size() > 0);
        assertThat("File should have no permission.", listStatus.get(0)
                .getPermission()
                .toString(), equalTo("---------"));
    }

    @Test
    public void testStatusUsingFilterFlow() throws Exception {

        for (String fileName : TestDataBuilder.fileNamesForListStatus()) {
            flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + fileName)
                    .withPayload(TestDataBuilder.payloadForWrite())
                    .run();
        }

        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY)
                        .withVariable("filter", ".*csv")
                        .run());

        assertThat(listStatus, notNullValue());
        assertThat(listStatus.size(), is(3));
    }

    @Test
    public void testStatusOwnerFlow() throws Exception {

        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH2)
                .withVariable("ownerUserName", OWNER)
                .withPayload(TestDataBuilder.payloadForWrite())
                .run();

        CoreEvent path = flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", FILE_PATH2)
                .run();
        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder.getValue(path);

        assertThat(listStatus, notNullValue());
        assertThat(listStatus.size(), is(1));
        assertThat("Wrong owner.", listStatus.get(0)
                .getOwner(), equalTo(OWNER));
    }
}
