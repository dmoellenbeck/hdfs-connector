/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.runtime.api.exception.MuleException;


@SuppressWarnings("unchecked")
public class RenameTestCase extends BaseTest {

    private static final String ROOT_DIRECTORY = "rootDirectory/";
    private static final String DIRECTORY = ROOT_DIRECTORY + "directory";
    private static final String NEW_DIRECTORY_NAME = ROOT_DIRECTORY + "newNameDirectory";
    private static final String MYFILE_PATH = ROOT_DIRECTORY + "myfile.txt";
    private static final String NEW_MYFILE_PATH = ROOT_DIRECTORY + "myfile1.txt";
    private byte[] writtenData;

    public RenameTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return TestConstants.RENAME_FLOW_PATH;
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadShortString();
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(writtenData))
                .run();
        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", DIRECTORY)
                .withVariable("permission", "700")
                .run();
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", ROOT_DIRECTORY)
                .run();
    }

    @Test
    public void testRenameDir() throws Exception {
        flowRunner(TestConstants.FlowNames.RENAME_FLOW).withVariable("source", DIRECTORY)
                .withVariable("destination", NEW_DIRECTORY_NAME)
                .run();
        List<FileStatus> destDirStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", NEW_DIRECTORY_NAME)
                        .run());
        assertThat(destDirStatuses, notNullValue());

        expectedException.expect(MuleException.class);
        List<FileStatus> sourceDirStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", DIRECTORY)
                        .run());
    }

    @Test
    public void testRenameFile() throws Exception {
        flowRunner(TestConstants.FlowNames.RENAME_FLOW).withVariable("source", MYFILE_PATH)
                .withVariable("destination", NEW_MYFILE_PATH)
                .run();
        List<FileStatus> destDirStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", NEW_MYFILE_PATH)
                        .run());
        assertThat(destDirStatuses, notNullValue());

        expectedException.expect(MuleException.class);

        TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", MYFILE_PATH)
                        .run());
    }

}
