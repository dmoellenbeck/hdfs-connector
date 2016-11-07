/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional.legacy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GetMetadataTestCases /* extends ConnectorTestCase */{

    @Before
    public void setUp() throws Exception {
        // initializeTestRunMessage("getMetadataTestData");
        // runFlowAndGetPayload("write-default-values");
    }

    @Test
    public void testGetMetadata() throws Exception {
        // MuleMessage muleMessage = runFlowAndGetMessage("get-metadata");

        // boolean exists = (Boolean) muleMessage.getInvocationProperty(HDFSConnector.HDFS_PATH_EXISTS);
        // assertTrue(exists);

        // MD5MD5CRC32FileChecksum fileMd5 = muleMessage.getInvocationProperty(HDFSConnector.HDFS_FILE_CHECKSUM);
        // assertNotNull(fileMd5);

        // FileStatus fileStatus = muleMessage.getInvocationProperty(HDFSConnector.HDFS_FILE_STATUS);
        // assertFalse(fileStatus.isDirectory());

        // ContentSummary contentSummary = muleMessage.getInvocationProperty(HDFSConnector.HDFS_CONTENT_SUMMARY);
        // assertNotNull(contentSummary);
    }

    @After
    public void tearDown() throws Exception {
        // runFlowAndGetMessage("delete-file");
    }

}
