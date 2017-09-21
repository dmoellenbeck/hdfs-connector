package org.mule.extension.hdfs.automation.functional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.extension.hdfs.api.MetaData;
import org.mule.extension.hdfs.util.TestConstants;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.core.api.construct.Flow;

import java.io.ByteArrayInputStream;

public class ReadSourceTestCases extends MuleArtifactFunctionalTestCase {
    private final String PARENT_DIRECTORY = "/";
    private final String MYFILE_PATH = "test.txt";

    @Override
    protected String getConfigFile() {
        return "flows/read-source-flow.xml";
    }


    @Before
    public void setUp() throws Exception {

       /* flowRunner(TestConstants.FlowNames.MAKE_DIR_FLOW).withVariable("path", PARENT_DIRECTORY)
                .withVariable("permission", "700")
                .run();*/
        flowRunner(TestConstants.FlowNames.WRITE_FLOW).withVariable("path", PARENT_DIRECTORY + MYFILE_PATH)
                .withPayload(new ByteArrayInputStream(TestDataBuilder.payloadShortString()))
                .run();
    }

    @Test
    public void thisShouldTestIfSourceIsWorking() throws Exception {
        ((Flow) getFlowConstruct("readSourceFlow")).start();
        Thread.sleep(2000);

        TypedValue retrievePayload = (TypedValue) muleContext.getObjectStoreManager().getObjectStore("hdfsStore").retrieve("payloadKey");
        String actualPayload = new String((byte[]) retrievePayload.getValue());

        TypedValue retrieveAttributes = (TypedValue) muleContext.getObjectStoreManager().getObjectStore("hdfsStore").retrieve("attrKey");
        MetaData actualAttributes = (MetaData) retrieveAttributes.getValue();

        Assert.assertTrue(actualPayload != null);
        Assert.assertTrue(actualAttributes != null);
        Assert.assertTrue(actualAttributes.isPathExists());
        Assert.assertTrue(actualPayload.length() == 5);


    }


}
