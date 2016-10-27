/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SetPermissionTestCases extends AbstractTestCases {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String MY_FILE = PARENT_DIRECTORY + "myFile.txt";
    private static final String OLD_PERMISSIONS = "754";
    private static final String NEW_PERMISSIONS = "700";
    private byte[] writtenData;

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadForSetPermissions();
        getConnector().write(MY_FILE, OLD_PERMISSIONS, true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
    }

    @Test
    public void testSetPermission() throws Exception {
        getConnector().setPermission(MY_FILE, NEW_PERMISSIONS);
        validateThatPermissionsWhereProperlySet();
    }

    private void validateThatPermissionsWhereProperlySet() throws Exception {
        FsPermission expectedPermission = new FsPermission(NEW_PERMISSIONS);
        List<FileStatus> fileStatuses = getConnector().listStatus(MY_FILE, null);
        FsPermission newPermission = fileStatuses.get(0)
                .getPermission();
        assertEquals("File permissions not properly set.", expectedPermission, newPermission);
    }

    @After
    public void tearDown() throws Exception {
        getConnector().deleteDirectory(PARENT_DIRECTORY);
    }
}
