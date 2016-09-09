/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.connection.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.mule.api.ConnectionException;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.param.ConnectionKey;

/**
 * Simple authentication configuration. Here you can configure properties required by "Simple Authentication" in order to establish connection with Hadoop Distributed File System.
 *
 * @author MuleSoft, Inc.
 */
@ConnectionManagement(friendlyName = "Simple Configuration")
public class Simple extends AbstractConfig {

    private HadoopClientConfigurationProvider hadoopClientConfigurationProvider;

    /**
     * Establish the connection to the Hadoop Distributed File System.
     *
     * @param nameNodeUri
     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
     *            in configurationResources and configurationEntries.
     * @throws org.mule.api.ConnectionException
     *             Holding information regarding reason of failure while trying to connect to the system.
     */
    @Connect
    @TestConnectivity
    public void connect(@ConnectionKey @FriendlyName("NameNode URI") final String nameNodeUri)
            throws ConnectionException {
        hadoopClientConfigurationProvider = new HadoopClientConfigurationProvider();
        final Configuration configuration = hadoopClientConfigurationProvider.forSimpleAuth(nameNodeUri, getUsername(), getConfigurationResources(), getConfigurationEntries());
        UserGroupInformation.setConfiguration(configuration);
        fileSystem(configuration);
    }
}
