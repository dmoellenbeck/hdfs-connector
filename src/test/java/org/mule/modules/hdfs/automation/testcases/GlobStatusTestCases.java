/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.testcases;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class GlobStatusTestCases extends HDFSTestParent {

    public static final String PATH_PREFIX = "/" + UUID.randomUUID()
            .toString() + "/";
    public static final String PATH_PATTERN = "pathPattern";
    public static final String INVALID_PATH_PATTERN = "invalidPathPattern";

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("globStatusTestData");
        for (String path : (List<String>) getTestRunMessageValue("pathList")) {
            upsertOnTestRunMessage("path", PATH_PREFIX + path);
            runFlowAndGetPayload("make-directories");
        }
    }

    @Category({ RegressionTests.class
    })
    @Test
    public void testGlobStatus() {
        try {
            prefixPathPattern();
            List<FileStatus> fileStatuses = runFlowAndGetPayload("glob-status");
            assertNotNull(fileStatuses);
            assertTrue((fileStatuses.get(0)
                    .getPath().toString()).contains(getTestRunMessageValue("result").toString()));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    private void prefixPathPattern() {
        String prefixedPathPattern = PATH_PREFIX + getTestRunMessageValue(PATH_PATTERN);
        upsertOnTestRunMessage(PATH_PATTERN, prefixedPathPattern);
    }

    @Category({ RegressionTests.class
    })
    @Test
    public void testGlobStatusWhenNoFileMatches() throws Exception {
        prefixPathPattern();
        upsertOnTestRunMessage("filter", new PathFilter() {

            @Override
            public boolean accept(Path path) {
                return false;
            }
        });
        List<FileStatus> fileStatuses = runFlowAndGetPayload("glob-status");
        assertNotNull(fileStatuses);
        assertThat(fileStatuses, Matchers.empty());
    }

    @Category({ RegressionTests.class
    })
    @Test
    public void testGlobStatusWhenNoFile() throws Exception {
        upsertOnTestRunMessage(PATH_PATTERN, INVALID_PATH_PATTERN);
        List<FileStatus> fileStatuses = runFlowAndGetPayload("glob-status");
        assertNotNull(fileStatuses);
        assertThat(fileStatuses, Matchers.empty());
    }

    @After
    public void tearDown() throws Exception {
        upsertOnTestRunMessage("path", PATH_PREFIX);
        runFlowAndGetPayload("delete-directory");
    }
}
