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

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SetPermissionTestCase extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String MY_FILE = PARENT_DIRECTORY + "myFile.txt";
    private static final String OLD_PERMISSIONS = "754";
    private static final String NEW_PERMISSIONS = "700";
    private final static Map<String, String> permissions = new HashMap<String, String>();
    static {
        permissions.put(NEW_PERMISSIONS, "rwx------");
    }

    private byte[] writtenData;

    @Override
    protected String getConfigFile() {
        return TestConstants.SET_PERMISSION_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadShortString();
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", MY_FILE)
                .withVariable("permission", OLD_PERMISSIONS)
                .withPayload(new ByteArrayInputStream(writtenData))
                .run();
    }

    @Test
    public void testSetPermission() throws Exception {
        flowRunner(TestConstants.FlowNames.SET_PERMISSION_FLOW).withVariable("path", MY_FILE)
                .withVariable("permission", NEW_PERMISSIONS)
                .run();
        validateThatPermissionsWhereProperlySet();
    }

    private void validateThatPermissionsWhereProperlySet() throws Exception {

        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", MY_FILE)
                        .run());

        assertThat("File permissions not properly set.", listStatus.get(0)
                .getPermission()
                .toString(), equalTo(permissions.get(NEW_PERMISSIONS)));

    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }
}
