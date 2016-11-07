/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;

/**
 * @author MuleSoft, Inc.
 */
public class KerberosTestCase {

    public static final String NAME_NODE_URI = "hdfs://localhost:9000";
    public static final String KEYTAB_PATH = "hdfs.keytab";
    @Mock
    private HdfsFileSystemProvider hdfsFileSystemProvider;
    @InjectMocks
    private Kerberos kerberos;

    @Test
    public void thatConnectIsReturningAValidConnection() throws Exception {
        kerberos.setNameNodeUri(NAME_NODE_URI);
        kerberos.setKeytabPath(KEYTAB_PATH);
        HdfsConnection hdfsConnection = kerberos.connect();
    }
}
