/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsConnectionBuilder;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionExceptionCode;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Kerberos authentication configuration. Here you can configure properties required by "Kerberos Authentication" in order to establish connection with Hadoop Distributed File
 * System.
 *
 */
@Alias("Kerberos")
public class Kerberos extends BaseProvider {

    private static final Logger logger = LoggerFactory.getLogger(Kerberos.class);
    /**
     * A simple user identity of a client process. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources
     * and configurationEntries.
     *
     */
    @Parameter
    @Optional
    @Placement(group = "Authentication")
    private String username;
    /**
     * Path to the <a href="https://web.mit.edu/kerberos/krb5-1.12/doc/basic/keytab_def.html">keytab file</a> associated with username. It is used in order to obtain TGT from
     * "Authorization server". If not provided it will look for a TGT associated to username within your local kerberos cache.
     *
     */
    @Parameter
    @Optional
    @Placement(group = "Authentication", order = 2)
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
    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client. Here you can provide additional configuration files. (e.g core-site.xml)
     *
     */
    @Parameter
    @Optional
    @Placement(group = "Advanced")
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client. Here you can provide additional configuration entries as key/value pairs.
     *
     */
    @Parameter
    @Optional
    @Placement(group = "Advanced")
    private Map<String, String> configurationEntries;
    @Inject
    private HdfsConnectionBuilder hdfsConnectionBuilder;

    /**
     * Sets keytabPath
     *
     * @param value
     *            Value to set
     */
    public void setKeytabPath(String value) {
        this.keytabPath = value;
    }

    /**
     * Retrieves keytabPath
     *
     */
    public String getKeytabPath() {
        return this.keytabPath;
    }

    /**
     * Sets nameNodeUri
     *
     * @param value
     *            Value to set
     */
    public void setNameNodeUri(String value) {
        this.nameNodeUri = value;
    }

    /**
     * Retrieves nameNodeUri
     *
     */
    public String getNameNodeUri() {
        return this.nameNodeUri;
    }

    @Override
    public HdfsConnection connect() throws ConnectionException {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forKerberosAuth(nameNodeUri, username, keytabPath, configurationResources, configurationEntries);
        checkConnect(hdfsConnection);
        return hdfsConnection;
    }

    @Override
    public void disconnect(HdfsConnection kerberos) {
        logger.debug("No action has to be taken in order to disconnect.");
    }

    @Override
    public ConnectionValidationResult validate(HdfsConnection kerberos) {
        try {
            checkFileSystem(kerberos);
            return ConnectionValidationResult.success();
        } catch (RuntimeIO e) {
            return ConnectionValidationResult.failure("Unable to establish connection with server", ConnectionExceptionCode.UNKNOWN, e);
        }
    }
}
