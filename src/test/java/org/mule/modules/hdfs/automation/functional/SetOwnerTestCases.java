/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SetOwnerTestCases extends AbstractTestCases {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String MY_FILE = PARENT_DIRECTORY + "myFile.txt";
    private static final String NEW_OWNER = "testUser";
    private static final String NEW_GROUP = "testGroup";
    private byte[] writtenData;

    @Before
    public void setUp() throws Exception {
        writtenData = TestDataBuilder.payloadForSetOwner();
        getConnector().write(MY_FILE, "755", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
    }

    @Test
    public void testSetOwner() throws Exception {
        getConnector().setOwner(MY_FILE, NEW_OWNER, NEW_GROUP);
        validateThatOwnerAndGroupWereProperlySet();
    }

    private void validateThatOwnerAndGroupWereProperlySet() throws Exception {
        List<FileStatus> fileStatus = getConnector().listStatus(MY_FILE, null);
        FileStatus status = fileStatus.get(0);
        String actualOwner = status.getOwner();
        String actualGroup = status.getGroup();
        assertThat("Owner was not properly set.", NEW_OWNER, is(actualOwner));
        assertThat("Group was not properly set.", NEW_GROUP, is(actualGroup));
    }

    @After
    public void tearDown() throws Exception {
        getConnector().deleteDirectory(PARENT_DIRECTORY);
    }
}
