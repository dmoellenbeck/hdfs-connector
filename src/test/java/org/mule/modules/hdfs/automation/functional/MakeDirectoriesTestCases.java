/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.junit.After;
import org.junit.Test;

public class MakeDirectoriesTestCases extends AbstractTestCases {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String NEW_DIRECTORY = "newDirectory";

    @Test
    public void testMakeDirectories() throws Exception {
        // getConnector().makeDirectories(PARENT_DIRECTORY + NEW_DIRECTORY, "700");
        // List<FileStatus> parentDirectoryStatuses = getConnector().listStatus(PARENT_DIRECTORY, null);
        // Assert.assertThat(parentDirectoryStatuses, notNullValue());
        // Assert.assertThat(parentDirectoryStatuses.get(0).getPath().getName(), is(NEW_DIRECTORY));
    }

    @After
    public void tearDown() throws Exception {
        // getConnector().deleteDirectory(PARENT_DIRECTORY);
    }

}
