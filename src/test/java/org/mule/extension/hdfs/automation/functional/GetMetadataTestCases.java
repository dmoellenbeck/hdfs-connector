/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.functional;

import org.hamcrest.core.StringContains;
import org.hamcrest.text.IsEmptyString;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.modules.hdfs.automation.functional.BaseTest;
import org.mule.runtime.core.exception.MessagingException;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@SuppressWarnings("unchecked")
public class GetMetadataTestCases extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String NEW_DIRECTORY = "newDirectory";
    @Rule
    public ExpectedException fileNotFoundExpected = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return TestConstants.GET_METADATA_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .withVariable("permission", "700")
                .run();
    }

    @After
    public void tearDown() throws Exception {
        flowRunner(TestConstants.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .run();
        verifyDeletetionOfDirectory();
    }

    @Test
    public void shouldContainDirectoryMetadataInfo() throws Exception {
        Map<String, Object> pathMetadata = (Map<String, Object>)TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.GET_METADATA_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY).run());

        //the checksum param is not populated since the path is a directory and not a file
        assertThat(pathMetadata, notNullValue());
        assertThat(pathMetadata.size(), is(3));
        assertThat(pathMetadata.get("hdfs.path.exists"), is(Boolean.TRUE));
        assertThat(String.valueOf(pathMetadata.get("hdfs.content.summary")), not(IsEmptyString.isEmptyOrNullString()));
        assertThat(String.valueOf(pathMetadata.get("hdfs.file.status")), not(IsEmptyString.isEmptyOrNullString()));
    }

    @Test
    public void shouldThrowExceptionForInvalidParameter() throws Exception {
        fileNotFoundExpected.expect(MessagingException.class);
        fileNotFoundExpected.expectMessage(StringContains.containsString(TestConstants.ExceptionMessages.ILLEGAL_PATH));
        flowRunner(TestConstants.FlowNames.GET_METADATA_FLOW).withVariable("path", null).run();
    }



    private void verifyDeletetionOfDirectory() throws Exception {
        Map<String, Object> pathMetadata = (Map<String, Object>)TestDataBuilder
                .getValue(flowRunner(TestConstants.FlowNames.GET_METADATA_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY).run());

        assertThat(pathMetadata, notNullValue());
        assertThat(pathMetadata.get("hdfs.path.exists"), is(Boolean.FALSE));
    }
}
