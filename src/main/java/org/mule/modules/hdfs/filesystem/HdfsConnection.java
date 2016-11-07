/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsConnection {

    private Map<String, String> configurationEntries;
    private List<String> configurationResources;
    private Map<String, String> additionalData;

    public HdfsConnection(Map<String, String> configurationEntries, List<String> configurationResources, Map<String, String> additionalData) {
        Objects.requireNonNull(configurationEntries, "Configuration entries can not be null");
        Objects.requireNonNull(configurationResources, "Configuration resources can not be null");
        Objects.requireNonNull(additionalData, "Additional data can not be null");
        this.configurationEntries = configurationEntries;
        this.configurationResources = configurationResources;
        this.additionalData = additionalData;
    }

    public Map<String, String> getConfigurationEntries() {
        return Collections.unmodifiableMap(configurationEntries);
    }

    public List<String> getConfigurationResources() {
        return Collections.unmodifiableList(configurationResources);
    }

    public Map<String, String> getAdditionalData() {
        return Collections.unmodifiableMap(additionalData);
    }
}
