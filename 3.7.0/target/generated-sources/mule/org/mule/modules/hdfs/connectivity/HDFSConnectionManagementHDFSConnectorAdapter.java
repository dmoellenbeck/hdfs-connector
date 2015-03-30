
package org.mule.modules.hdfs.connectivity;

import javax.annotation.Generated;
import org.mule.api.ConnectionException;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectionAdapter;
import org.mule.modules.hdfs.connection.strategy.HDFSConnectionManagement;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.6.1", date = "2015-03-30T02:49:38-03:00", comments = "Build UNNAMED.2405.44720b7")
public class HDFSConnectionManagementHDFSConnectorAdapter
    extends HDFSConnectionManagement
    implements ConnectionManagementConnectionAdapter<HDFSConnectionManagement, ConnectionManagementConfigHDFSConnectorConnectionKey>
{


    @Override
    public void connect(ConnectionManagementConfigHDFSConnectorConnectionKey connectionKey)
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

    @Override
    public HDFSConnectionManagement getStrategy() {
        return this;
    }

}
