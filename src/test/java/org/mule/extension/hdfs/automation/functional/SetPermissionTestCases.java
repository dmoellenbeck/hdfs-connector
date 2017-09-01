package org.mule.extension.hdfs.automation.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.Permissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;

public class SetPermissionTestCases extends BaseTest {

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
        return Util.SET_PERMISSION_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadShortString();
        flowRunner(Util.FlowNames.WRITE_FLOW).withVariable("path", MY_FILE)
                .withVariable("permission", OLD_PERMISSIONS)
                .withPayload(new ByteArrayInputStream(writtenData))
                .run();
    }

    @Test
    public void testSetPermission() throws Exception {
        flowRunner(Util.FlowNames.SET_PERMISSION_FLOW).withVariable("path", MY_FILE)
                .withVariable("permission", NEW_PERMISSIONS)
                .run();
        validateThatPermissionsWhereProperlySet();
    }

    private void validateThatPermissionsWhereProperlySet() throws Exception {

        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", MY_FILE)
                        .run());

        assertThat("File permissions not properly set.", listStatus.get(0)
                .getPermission()
                .toString(), equalTo(permissions.get(NEW_PERMISSIONS)));

    }

    @After
    public void tearDown() throws Exception {
        flowRunner(Util.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }
}
