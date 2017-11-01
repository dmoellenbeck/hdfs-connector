/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;

public class SetOwnerTestCase extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String MY_FILE = PARENT_DIRECTORY + "myFile.txt";
    private static final String NEW_OWNER = "testUser";
    private static final String NEW_GROUP = "testGroup";
    private byte[] writtenData;

    public SetOwnerTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return TestConstants.SET_OWNER_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadShortString();
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", MY_FILE)
                .withVariable("permission", "755")
                .withPayload(new ByteArrayInputStream(writtenData))
                .run();
    }

    @Test
    public void testSetOwner() throws Exception {
        flowRunner(TestConstants.FlowNames.SET_OWNER_FLOW).withVariable("path", MY_FILE)
                .withVariable("ownername", NEW_OWNER)
                .withVariable("groupname", NEW_GROUP)
                .run();

        validateThatOwnerAndGroupWereProperlySet();
    }

    private void validateThatOwnerAndGroupWereProperlySet() throws Exception {
        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", MY_FILE)
                        .run());

        assertThat("Owner was not properly set.", listStatus.get(0)
                .getOwner(), equalTo(NEW_OWNER));
        assertThat("Group was not properly set.", listStatus.get(0)
                .getGroup(), equalTo(NEW_GROUP));
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }
}
