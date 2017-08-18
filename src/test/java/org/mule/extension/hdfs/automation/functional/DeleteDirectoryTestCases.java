package org.mule.extension.hdfs.automation.functional;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.extension.hdfs.util.Util;
import org.mule.modules.hdfs.automation.functional.BaseTest;

@SuppressWarnings("unchecked")
public class DeleteDirectoryTestCases extends BaseTest {

    private static final String PARENT_DIRECTORY = "rootDirectory/";
    private static final String NEW_DIRECTORY = "newDirectory";
    @Rule
    public ExpectedException fileNotFoundExpected = ExpectedException.none();

    @Override
    protected String getConfigFile() {
        return Util.DELETE_DIR_FLOW_PATH;
    }

    @Before
    public void setUp() throws Exception {
        flowRunner(Util.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                .withVariable("permission", "700")
                .run();
    }
    
  @Test
  public void testDeleteDirectory() throws Exception {
      flowRunner(Util.FlowNames.DELETE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
      .run();
      verifyDeletetionOfDirectory();
  }

    private void verifyDeletetionOfDirectory() throws Exception {
        fileNotFoundExpected.expect(Exception.class);
        List<FileStatus> listStatus = (List<FileStatus>) TestDataBuilder
                .getValue(flowRunner(Util.FlowNames.LIST_STATUS_FLOW).withVariable("path", PARENT_DIRECTORY + NEW_DIRECTORY)
                        .run());
    }
}
