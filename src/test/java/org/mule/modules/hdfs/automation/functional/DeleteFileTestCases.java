/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeleteFileTestCases extends AbstractTestCases {

    private static final String PARENT_DIRECTORY = "rootDirecotry/";
    private static final String MYFILE_PATH = PARENT_DIRECTORY + "myfile.txt";

    @Before
    public void setUp() throws Exception {
        // getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(TestDataBuilder.payloadForDelete()));
    }

    @After
    public void tearDown() throws Exception {
        // getConnector().deleteFile(PARENT_DIRECTORY);
    }

    @Test
    public void testDeleteFile() throws Exception {
        // getConnector().deleteFile(MYFILE_PATH);
        // List<FileStatus> parentDirectoryStatuses = getConnector().listStatus(PARENT_DIRECTORY, null);
        // Assert.assertThat(parentDirectoryStatuses, notNullValue());
        // Assert.assertThat(parentDirectoryStatuses, empty());
    }

}
