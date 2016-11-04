/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.connection.config;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kerberos authentication configuration. Here you can configure properties required by "Kerberos Authentication" in order to establish connection with Hadoop Distributed File
 * System.
 *
 * @author MuleSoft, Inc.
 */
@ConnectionManagement(configElementName = "config-with-kerberos", friendlyName = "Kerberos Configuration")
public class Kerberos extends AbstractConfig {

    private static final Logger logger = LoggerFactory.getLogger(Kerberos.class);

    /**
     * Kerberos principal. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources and
     * configurationEntries.
     */
    @Configurable
    @Optional
    @Placement(order = 1, group = "Authentication")
    private String username; // we call it username for backward compatibility reasons in terms of what will be seen in xml

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Path to the <a href="https://web.mit.edu/kerberos/krb5-1.12/doc/basic/keytab_def.html">keytab file</a> associated with username. It is used in order to obtain TGT from
     * "Authorization server". If not provided it will look for a TGT associated to username within your local kerberos cache.
     */
    @Configurable
    @Optional
    @Placement(order = 2, group = "Authentication")
    @FriendlyName("Keytab File")
    private String keytabPath;
    private HadoopClientConfigurationProvider hadoopClientConfigurationProvider;

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }

    /**
     * Establish the connection to the Hadoop Distributed File System.
     *
     * @param nameNodeUri
     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
     *            in configurationResources and configurationEntries.
     * @throws org.mule.api.ConnectionException
     *             Holding information regarding reason of failure while trying to connect to the system.
     */
    @Connect
    @TestConnectivity
    public void connect(@ConnectionKey @FriendlyName("NameNode URI") final String nameNodeUri) throws ConnectionException {
        hadoopClientConfigurationProvider = new HadoopClientConfigurationProvider();
        final Configuration configuration = hadoopClientConfigurationProvider.forKerberosAuth(nameNodeUri, username, getConfigurationResources(), getConfigurationEntries());
        UserGroupInformation.setConfiguration(configuration);
        if (isKeytabProvided()) {
            loginUserUsingKeytab();
        }
        fileSystem(configuration);
    }

    private boolean isKeytabProvided() {
        return StringUtils.isNotEmpty(getKeytabPath());
    }

    private void loginUserUsingKeytab() throws ConnectionException {
        try {
            UserGroupInformation.loginUserFromKeytab(username, getKeytabPath());
        } catch (IOException e) {
            logger.error("Unable to login user using keytab", e);
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null, "Unable to login user using keytab", e);
        }
    }

}
