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
