/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.unit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.extension.hdfs.internal.connection.provider.util.HadoopConfigurationUtil;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;

public class ConnectionTest {
    HadoopConfigurationUtil hadoopConfigurationUtil=new HadoopConfigurationUtil();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Test
    public void TestKerberosWithNullNameNodeParams()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"nameNodeUri\" can not be null or empty");
        hadoopConfigurationUtil.getKerberosAuthConfig(null,null,null,null);

    }
    @Test
    public void testKerberosWithNullPrincipal()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"principal\" can not be null or empty.");
        hadoopConfigurationUtil.getKerberosAuthConfig("DA",null,null,null);
    }

}
