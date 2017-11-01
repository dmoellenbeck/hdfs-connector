/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.runtime.api.exception.MuleException;


@SuppressWarnings("unchecked")
public class DeleteDirectoryTestCase extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String NEW_DIRECTORY = "newDirectory";
    private static final String UNEXISTING_FILE = "unexisting.txt";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public DeleteDirectoryTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
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
        verifyDeletionOfDirectory();
    }

    @Test
    public void testDeleteUnexistingDir() throws Exception {

        expectedException.expect(MuleException.class);
        expectedException.expectMessage(StringContains.containsString(ExceptionMessages.UNABLE_TO_DELETE_DIR));

        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", UNEXISTING_FILE)
                .run();
    }

    private void verifyDeletionOfDirectory() throws Exception {
        expectedException.expect(MuleException.class);
        expectedException.expectMessage(StringContains.containsString(TestConstants.ExceptionMessages.INVALID_FILE_PATH));
        flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .run();
    }
}
