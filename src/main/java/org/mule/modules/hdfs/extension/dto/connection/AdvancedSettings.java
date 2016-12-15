/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.dto.connection;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

import java.util.List;
import java.util.Map;

/**
 * @author MuleSoft, Inc.
 */
public class AdvancedSettings {

    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client. Here you can provide additional configuration files. (e.g core-site.xml)
     *
     */
    @Parameter
    @Optional
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client. Here you can provide additional configuration entries as key/value pairs.
     *
     */
    @Parameter
    @Optional
    private Map<String, String> configurationEntries;

    public List<String> getConfigurationResources() {
        return configurationResources;
    }

    public void setConfigurationResources(List<String> configurationResources) {
        this.configurationResources = configurationResources;
    }

    public Map<String, String> getConfigurationEntries() {
        return configurationEntries;
    }

    public void setConfigurationEntries(Map<String, String> configurationEntries) {
        this.configurationEntries = configurationEntries;
    }
}
