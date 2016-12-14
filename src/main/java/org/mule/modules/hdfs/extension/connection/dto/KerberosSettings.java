/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.dto;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

/**
 * @author MuleSoft, Inc.
 */
public class KerberosSettings {

    /**
     * A simple user identity of a client process. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources
     * and configurationEntries.
     *
     */
    @Parameter
    @Optional
    private String principal;
    /**
     * Path to the <a href="https://web.mit.edu/kerberos/krb5-1.12/doc/basic/keytab_def.html">keytab file</a> associated with principal. It is used in order to obtain TGT from
     * "Authorization server". If not provided it will look for a TGT associated to principal within your local kerberos cache.
     *
     */
    @Parameter
    @Optional
    @DisplayName("Keytab File")
    private String keytabPath;
    /**
     *
     * @param nameNodeUri
     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
     *            in configurationResources and configurationEntries.
     */
    @Parameter
    @DisplayName("NameNode URI")
    public String nameNodeUri;

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }

    public String getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(String nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }
}
