/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs;

import org.junit.Test;
import org.mule.modules.hdfs.exception.HDFSConnectorException;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HDFSConnectorExceptionTest {

    @Test
    public void testHDFSConnectorException() {
        try {
            final String EX_MESSAGE = "Testing Exception";
            Exception dummyException = new Exception();
            HDFSConnectorException hdfsConnectorException;
            hdfsConnectorException = new HDFSConnectorException(EX_MESSAGE);
            assertEquals(EX_MESSAGE, hdfsConnectorException.getMessage());

            hdfsConnectorException = new HDFSConnectorException(dummyException);
            assertEquals(dummyException, hdfsConnectorException.getCause());

            hdfsConnectorException = new HDFSConnectorException(EX_MESSAGE, dummyException);
            assertEquals(EX_MESSAGE, hdfsConnectorException.getMessage());
            assertEquals(dummyException, hdfsConnectorException.getCause());
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }
}
