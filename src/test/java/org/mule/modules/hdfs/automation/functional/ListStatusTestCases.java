/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ListStatusTestCases extends AbstractTestCases {

    private static final String PARENT_DIRECTORY = "rootDirectory/";

    @Before
    public void setUp() throws Exception {
        for (String childFile : TestDataBuilder.fileNamesForListStatus()) {
            getConnector().write(PARENT_DIRECTORY + childFile, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(TestDataBuilder.payloadForListStatus()));
        }
    }

    @Test
    public void testListStatus() throws Exception {
        List<FileStatus> fileStatuses = getConnector().listStatus(PARENT_DIRECTORY, ".*txt");
        Assert.assertThat(fileStatuses, notNullValue());
        Assert.assertThat(fileStatuses.size(), is(3));
    }

    @After
    public void tearDown() throws Exception {
        getConnector().deleteDirectory(PARENT_DIRECTORY);
    }
}
