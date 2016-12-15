/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.mule.modules.hdfs.extension.dto.connection.AdvancedSettings;
import org.mule.modules.hdfs.extension.dto.connection.SimpleSettings;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsConnectionBuilder;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.ConfigName;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Simple authentication configuration. Here you can configure properties required by "Simple Authentication" in order to establish connection with Hadoop Distributed File System.
 *
 */
@DisplayName("Simple configuration")
@Alias("simple")
public class Simple extends BaseProvider {

    private static final Logger logger = LoggerFactory.getLogger(Simple.class);

    @ConfigName
    private String configName;
    @ParameterGroup("Authentication")
    private SimpleSettings simpleSettings;
    @ParameterGroup("Advanced")
    private AdvancedSettings advancedSettings;
    @Inject
    HdfsConnectionBuilder hdfsConnectionBuilder = new HdfsConnectionBuilder();

    @Override
    public HdfsConnection connect() throws ConnectionException {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth(simpleSettings.getNameNodeUri(), simpleSettings.getUsername(),
                advancedSettings.getConfigurationResources(), advancedSettings.getConfigurationEntries());
        checkConnect(hdfsConnection);
        return hdfsConnection;
    }

    @Override
    public void disconnect(HdfsConnection connection) {
        logger.debug("No action has to be taken in order to disconnect.");
    }

    void setAdvancedSettings(AdvancedSettings advancedSettings) {
        this.advancedSettings = advancedSettings;
    }

    void setSimpleSettings(SimpleSettings simpleSettings) {
        this.simpleSettings = simpleSettings;
    }
}
