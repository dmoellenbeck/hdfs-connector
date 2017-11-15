/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.connection.param;

import java.util.List;
import java.util.Map;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.Placement;

public class KerberosConnectionParams {

    /**
     * Kerberos principal. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources and
     * configurationEntries. We call it username for backward compatibility reasons in terms of what will be seen in xml
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.CONNECTION_TAB, order = 1)
    private String username;

    /**
     * Path to the <a href="https://web.mit.edu/kerberos/krb5-1.12/doc/basic/keytab_def.html">keytab file</a> associated with username. It is used in order to obtain TGT from
     * "Authorization server". If not provided it will look for a TGT associated to username within your local kerberos cache.
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.CONNECTION_TAB, order = 2)
    private String keytabPath;

    /**
     * The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values in
     * configurationResources and configurationEntries.
     */
    @Parameter
    @Placement(tab = Placement.CONNECTION_TAB, order = 3)
    private String nameNodeUri;

    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client. Here you can provide additional configuration files. (e.g core-site.xml)
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.ADVANCED_TAB, order = 4)
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client. Here you can provide additional configuration entries as key/value pairs.
     */
    @Parameter
    @Optional
    @Placement(tab = Placement.ADVANCED_TAB, order = 5)
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

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }

}
