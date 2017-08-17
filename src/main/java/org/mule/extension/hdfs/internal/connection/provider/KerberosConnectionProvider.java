/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.connection.provider;

import static org.mule.runtime.extension.api.annotation.param.ParameterGroup.CONNECTION;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.mule.extension.hdfs.api.connection.param.KerberosConnectionParams;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.connection.provider.util.HadoopConfigurationUtil;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author MuleSoft, Inc.
 */
@Alias("kerberos")
@DisplayName("Kerberos")
public class KerberosConnectionProvider extends FileSystemConnectionProvider implements ConnectionProvider<HdfsConnection> {

    private static final Logger logger = LoggerFactory.getLogger(KerberosConnectionProvider.class);

    @ParameterGroup(name = CONNECTION)
    @Placement(order = 1)
    private KerberosConnectionParams kerberosConnectionParams;

    /**
     * Connection for Kerberos authentication in order to establish connection with Hadoop Distributed File System.
     */
    @Override
    public HdfsConnection connect() throws ConnectionException {
        try {
            HadoopConfigurationUtil hadoopConfigProvider = new HadoopConfigurationUtil();
            final Configuration configuration = hadoopConfigProvider.getSimpleAuthConfig(kerberosConnectionParams.getNameNodeUri(), kerberosConnectionParams.getUsername(),
                    kerberosConnectionParams.getConfigurationResources(), kerberosConnectionParams.getConfigurationEntries());
            UserGroupInformation.setConfiguration(configuration);
            if (StringUtils.isNotEmpty(kerberosConnectionParams.getKeytabPath())) {
                UserGroupInformation.loginUserFromKeytab(kerberosConnectionParams.getUsername(), kerberosConnectionParams.getKeytabPath());
            }
            return new FileSystemConnection(FileSystem.get(configuration));
        } catch (Exception e) {
            throw new ConnectionException(e.getMessage(), e);
        }
    }

}
