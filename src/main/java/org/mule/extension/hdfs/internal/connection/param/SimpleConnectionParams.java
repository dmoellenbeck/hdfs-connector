/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.connection.param;

import java.util.List;
import java.util.Map;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.Placement;

public class SimpleConnectionParams {

    /**
     * User identity that Hadoop uses for permissions in HDFS. <br/>
     * When Simple Authentication is used, Hadoop requires the user to be set as a System Property called HADOOP_USER_NAME. If you fill this field then the connector will set it
     * for you, however you can set it by yourself. If the variable is not set, Hadoop will use the current logged in OS user.
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.CONNECTION_TAB, order = 1)
    private String username;

    /**
     * The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values in
     * configurationResources and configurationEntries.
     */
    @Parameter
    @Placement(tab = Placement.CONNECTION_TAB, order = 2)
    private String nameNodeUri;

    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client. Here you can provide additional configuration files. (e.g core-site.xml)
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.ADVANCED_TAB, order = 3)
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client. Here you can provide additional configuration entries as key/value pairs.
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.ADVANCED_TAB, order = 4)
    private Map<String, String> configurationEntries;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

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

    public String getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(String nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }

}
