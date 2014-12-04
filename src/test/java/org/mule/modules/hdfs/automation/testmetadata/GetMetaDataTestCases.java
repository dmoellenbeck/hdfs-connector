package org.mule.modules.hdfs.automation.testmetadata;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.api.processor.MessageProcessor;
import org.mule.common.Result;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.OperationMetaDataEnabled;
import org.mule.common.metadata.datatype.DataType;
import org.mule.construct.Flow;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.SmokeTests;
import org.mule.modules.hdfs.automation.HDFSTestParent;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.*;

public class GetMetaDataTestCases extends HDFSTestParent {

    @Test
    @Category({RegressionTests.class, SmokeTests.class})
    public void testGetMetaDataRename() {
        try {
            Flow flow = (Flow) muleContext.getRegistry().lookupFlowConstruct("get-meta-data-rename");
            assertFalse(flow.getMessageProcessors().isEmpty());
            MessageProcessor messageProcessor = flow.getMessageProcessors().get(0);
            assertThat(messageProcessor, CoreMatchers.instanceOf(OperationMetaDataEnabled.class));

            Result<MetaData> inputMetaData = ((OperationMetaDataEnabled) messageProcessor).getInputMetaData();
            assertEquals(Result.Status.SUCCESS, inputMetaData.getStatus());

            Result<MetaData> outputMetaData = ((OperationMetaDataEnabled) messageProcessor).getOutputMetaData(null);
            assertEquals(Result.Status.SUCCESS, outputMetaData.getStatus());

            assertEquals(DataType.BOOLEAN, outputMetaData.get().getPayload().getDataType());

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @Test
    @Category({RegressionTests.class, SmokeTests.class})
    public void testGetMetaDataListStatus() {
        try {
            Flow flow = (Flow) muleContext.getRegistry().lookupFlowConstruct("get-meta-data-list-status");
            assertFalse(flow.getMessageProcessors().isEmpty());
            MessageProcessor messageProcessor = flow.getMessageProcessors().get(0);
            assertThat(messageProcessor, CoreMatchers.instanceOf(OperationMetaDataEnabled.class));

            Result<MetaData> inputMetaData = ((OperationMetaDataEnabled) messageProcessor).getInputMetaData();
            assertEquals(Result.Status.SUCCESS, inputMetaData.getStatus());

            Result<MetaData> outputMetaData = ((OperationMetaDataEnabled) messageProcessor).getOutputMetaData(null);
            assertEquals(Result.Status.SUCCESS, outputMetaData.getStatus());

            assertEquals(DataType.LIST, outputMetaData.get().getPayload().getDataType());

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @Test
    @Category({RegressionTests.class, SmokeTests.class})
    public void testGetMetaDataGlobStatus() {
        try {
            Flow flow = (Flow) muleContext.getRegistry().lookupFlowConstruct("get-meta-data-glob-status");
            assertFalse(flow.getMessageProcessors().isEmpty());
            MessageProcessor messageProcessor = flow.getMessageProcessors().get(0);
            assertThat(messageProcessor, CoreMatchers.instanceOf(OperationMetaDataEnabled.class));

            Result<MetaData> inputMetaData = ((OperationMetaDataEnabled) messageProcessor).getInputMetaData();
            assertEquals(Result.Status.SUCCESS, inputMetaData.getStatus());

            Result<MetaData> outputMetaData = ((OperationMetaDataEnabled) messageProcessor).getOutputMetaData(null);
            assertEquals(Result.Status.SUCCESS, outputMetaData.getStatus());

            assertEquals(DataType.LIST, outputMetaData.get().getPayload().getDataType());

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }
}
