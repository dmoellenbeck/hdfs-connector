/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.mule.modules.hdfs.extension.connection.dto.AdvancedSettings;
import org.mule.modules.hdfs.extension.connection.dto.KerberosSettings;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsConnectionBuilder;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.ConfigName;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Kerberos authentication configuration. Here you can configure properties required by "Kerberos Authentication" in order to establish connection with Hadoop Distributed File
 * System.
 *
 */
@DisplayName("Kerberos configuration")
@Alias("kerberos")
public class Kerberos extends BaseProvider implements ConnectionProvider<HdfsConnection> {

    private static final Logger logger = LoggerFactory.getLogger(Kerberos.class);

    @ConfigName
    private String configName;
    @ParameterGroup("Auhentication")
    private KerberosSettings kerberosSettings;
    @ParameterGroup("Advanced")
    private AdvancedSettings advancedSettings;
    @Inject
    private HdfsConnectionBuilder hdfsConnectionBuilder;

    void setKerberosSettings(KerberosSettings kerberosSettings) {
        this.kerberosSettings = kerberosSettings;
    }

    void setAdvancedSettings(AdvancedSettings advancedSettings) {
        this.advancedSettings = advancedSettings;
    }

    @Override
    public HdfsConnection connect() throws ConnectionException {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forKerberosAuth(kerberosSettings.getNameNodeUri(), kerberosSettings.getPrincipal(),
                kerberosSettings.getKeytabPath(), advancedSettings.getConfigurationResources(), advancedSettings.getConfigurationEntries());
        checkConnect(hdfsConnection);
        return hdfsConnection;
    }

    @Override
    public void disconnect(HdfsConnection connection) {
        logger.debug("No action has to be taken in order to disconnect.");
    }
}
