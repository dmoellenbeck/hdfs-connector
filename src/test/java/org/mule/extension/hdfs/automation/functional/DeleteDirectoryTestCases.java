/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import java.io.FileNotFoundException;
import java.util.List;

import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.modules.hdfs.automation.functional.BaseTest;
import org.mule.runtime.core.exception.MessagingException;

@SuppressWarnings("unchecked")
public class DeleteDirectoryTestCases extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String NEW_DIRECTORY = "newDirectory";
    @Rule
    public ExpectedException fileNotFoundExpected = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return TestConstants.DELETE_DIR_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .withVariable("permission", "700")
                .run();
    }

    @Test
    public void testDeleteDirectory() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .run();
        verifyDeletetionOfDirectory();
    }

    private void verifyDeletetionOfDirectory() throws Exception {
        fileNotFoundExpected.expect(MessagingException.class);
        fileNotFoundExpected.expectMessage(StringContains.containsString(TestConstants.ExceptionMessages.INVALID_FILE_PATH));
        flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                        .run();
    }
}
