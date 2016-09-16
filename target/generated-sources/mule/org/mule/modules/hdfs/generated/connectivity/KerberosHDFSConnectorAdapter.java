
package org.mule.modules.hdfs.generated.connectivity;

import javax.annotation.Generated;
import org.mule.api.ConnectionException;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectionAdapter;
import org.mule.devkit.internal.connection.management.TestableConnection;
import org.mule.modules.hdfs.connection.config.Kerberos;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class KerberosHDFSConnectorAdapter
    extends Kerberos
    implements ConnectionManagementConnectionAdapter<Kerberos, ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey> , TestableConnection<ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey>
{


    @Override
    public Kerberos getStrategy() {
        return this;
    }

    @Override
    public void test(ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey connectionKey)
        throws ConnectionException
    {
        super.connect(connectionKey.getNameNodeUri());
    }

    @Override
    public void connect(ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey connectionKey)
        throws ConnectionException
    {
        super.connect(connectionKey.getNameNodeUri());
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    @Override
    public String connectionId() {
        return super.getFileSystemUri();
    }

    @Override
    public boolean isConnected() {
        return super.isConnected();
    }

}
