/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
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

public class ReadSourceTestCase extends BaseTest {
    private final String PARENT_DIRECTORY = "/";
    private final String MYFILE_PATH = "test.txt";

    public ReadSourceTestCase(String configuration) {
        super(configuration);
    }

    @Override
    public String getFlowFile() {
        return "flows/read-source-flow.xml";
    }


    @Before
    public void setUp() throws Exception {

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
