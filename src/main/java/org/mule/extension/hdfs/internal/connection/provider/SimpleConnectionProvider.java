/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.connection.provider;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.mule.extension.hdfs.internal.connection.param.SimpleConnectionParams;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.connection.provider.util.HadoopConfigurationUtil;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Placement;

import static org.mule.runtime.extension.api.annotation.param.ParameterGroup.CONNECTION;

/**
 * @author MuleSoft, Inc.
 */
@Alias("simple")
@DisplayName("Simple")
public class SimpleConnectionProvider extends FileSystemConnectionProvider implements CachedConnectionProvider<HdfsConnection> {



    @ParameterGroup(name = CONNECTION)
    @Placement(order = 1)
    private SimpleConnectionParams simpleConnectionParams;

    /**
     * Connection for simple authentication. Here you can configure properties required by "Simple Authentication" in order to establish connection with Hadoop Distributed File
     * System.
     */
    @Override
    public HdfsConnection connect() throws ConnectionException {
        try {
            putProvidedUsernameAsSystemProperty();
            HadoopConfigurationUtil hadoopConfigProvider = new HadoopConfigurationUtil();
            final Configuration configuration = hadoopConfigProvider.getSimpleAuthConfig(simpleConnectionParams.getNameNodeUri(), simpleConnectionParams.getUsername(),
                    simpleConnectionParams.getConfigurationResources(), simpleConnectionParams.getConfigurationEntries());
            UserGroupInformation.setConfiguration(configuration);
            return new FileSystemConnection(FileSystem.get(configuration));
        } catch (Exception e) {
            throw new ConnectionException(e.getMessage(), e);
        }
    }

    private void putProvidedUsernameAsSystemProperty() {
        if (StringUtils.isNotEmpty(simpleConnectionParams.getUsername())) {
            System.setProperty("HADOOP_USER_NAME", simpleConnectionParams.getUsername());
        }
    }
}
