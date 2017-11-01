/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.core.api.event.CoreEvent;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;


@SuppressWarnings("unchecked")
public class DeleteFileTestCase extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirecotry/";
    private static final String MYFILE_PATH = PARENT_DIRECTORY + "myfile.txt";
    private static final String UNEXISTING_FILE = "unexisting.txt";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public DeleteFileTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return TestConstants.DELETE_FILE_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {

        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("permission", "700")
                .run();
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(TestDataBuilder.payloadShortString()))
                .run();
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .run();
    }

    @Test
    public void testDeleteFile() throws Exception {
      
        flowRunner(TestConstants.FlowNames.DELETE_FILE_FLOW).withVariable("path", MYFILE_PATH)
                .run();

        CoreEvent path = flowRunner(TestConstants.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY).run();
        List<FileStatus> parentDirectoryStatuses = (List<FileStatus>) TestDataBuilder
                .getValue(path);
        assertThat(parentDirectoryStatuses, notNullValue());
        assertThat(parentDirectoryStatuses, empty());
    }
    
    @Test
    public void testDeleteUnexistingFile() throws Exception {
      
        expectedException.expect(MuleException.class);
        expectedException.expectMessage(StringContains.containsString(ExceptionMessages.UNABLE_TO_DELETE_FILE));
        
        flowRunner(TestConstants.FlowNames.DELETE_FILE_FLOW).withVariable("path", UNEXISTING_FILE)
                .run();
    }

}
