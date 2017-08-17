/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;

public class ListStatusTestCases extends BaseTest {

    private final String PARENT_DIRECTORY = "hdfs-test-list-status/";
    private final String FILE_PATH1 = PARENT_DIRECTORY + "hdfs-test-write-file1.txt";
    private final String FILE_PATH2 = PARENT_DIRECTORY + "hdfs-test-write-file2.txt";

    private final String OWNER = "adriaan";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return Util.LIST_STATUS_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        flowRunner(Util.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("permission", "700")
                .run();
        for (String fileName : TestDataBuilder.fileNamesForGlobStatus()) {
            flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + fileName)
                    .withPayload(TestDataBuilder.payloadForWrite())
                    .run();
        }
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(Util.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }

    @Test
    public void testStatusPermissionFlow() throws Exception {

        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH1)
                .withVariable("permission", "000")
                .withPayload(TestDataBuilder.payloadForWrite())
                .run();

        List<FileStatus> listStatus = (List<FileStatus>) flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", FILE_PATH1)
                .withPayload(TestDataBuilder.payloadForWrite())
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        assertThat("It should be at list one file.", listStatus.size() > 0);
        assertThat("File should have no permission.", listStatus.get(0)
                .getPermission()
                .toString(), equalTo("---------"));
    }

    @Test
    public void testStatusUsingFilterFlow() throws Exception {

        for (String fileName : TestDataBuilder.fileNamesForListStatus()) {
            flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + fileName)
                    .withPayload(TestDataBuilder.payloadForWrite())
                    .run();
        }

        List<FileStatus> listStatus = (List<FileStatus>) flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("filter", ".*csv")
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        assertThat(listStatus, notNullValue());
        assertThat(listStatus.size(), is(3));
    }

    @Test
    public void testStatusOwnerFlow() throws Exception {

        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", FILE_PATH2)
                .withVariable("ownerUserName", OWNER)
                .withPayload(TestDataBuilder.payloadForWrite())
                .run();

        List<FileStatus> listStatus = (List<FileStatus>) flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", FILE_PATH2)
                .run()
                .getMessage()
                .getPayload()
                .getValue();

        assertThat(listStatus, notNullValue());
        assertThat(listStatus.size(), is(1));
        assertThat("Wrong owner.", listStatus.get(0)
                .getOwner(), equalTo(OWNER));
    }
}
