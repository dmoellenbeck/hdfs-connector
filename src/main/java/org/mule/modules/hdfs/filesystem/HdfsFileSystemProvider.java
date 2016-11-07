/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.mule.modules.hdfs.filesystem.exception.AuthenticationFailed;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystemProvider {

    public MuleFileSystem fileSystem(HdfsConnection hdfsConnection) {
        Configuration configuration = new HdfsConfigurationBuilder().build(hdfsConnection);
        HdfsAuthenticator.authenticate(configuration, hdfsConnection);
        try {
            return new HdfsFileSystem(FileSystem.get(configuration));
        } catch (IOException e) {
            throw new RuntimeIO("Unable to obtain file system.", e);
        }
    }

    static class HdfsConfigurationBuilder {

        private Configuration configuration;

        public HdfsConfigurationBuilder() {
            configuration = new Configuration();
        }

        public Configuration build(HdfsConnection hdfsConnection) {
            hdfsConnection.getConfigurationEntries().entrySet().forEach(configurationEntry -> configuration.set(configurationEntry.getKey(), configurationEntry.getValue()));
            hdfsConnection.getConfigurationResources().forEach(configurationResource -> configuration.addResource(configurationResource));
            return configuration;
        }
    }

    static class HdfsAuthenticator {

        private static Logger logger = LoggerFactory.getLogger(HdfsAuthenticator.class);

        public static void authenticate(Configuration configuration, HdfsConnection hdfsConnection) {
            if (shouldITryToLoginUsingKeytab(hdfsConnection)) {
                try {
                    logger.info("Trying to authenticate using keytab...");
                    UserGroupInformation.setConfiguration(configuration);
                    UserGroupInformation.loginUserFromKeytab(obtainPrincipal(hdfsConnection), obtainKeytab(hdfsConnection));
                } catch (IOException e) {
                    throw new AuthenticationFailed("Unable to authenticate user using kerberos.", e);
                }
            }
        }

        private static boolean shouldITryToLoginUsingKeytab(HdfsConnection hdfsConnection) {
            return isKerberosAuth(hdfsConnection) && isKeytabProvided(hdfsConnection);
        }

        private static String obtainKeytab(HdfsConnection hdfsConnection) {
            return hdfsConnection.getAdditionalData()
                    .get("hadoop.security.kerberos.keytab");
        }

        private static String obtainPrincipal(HdfsConnection hdfsConnection) {
            return hdfsConnection.getConfigurationEntries()
                    .get("hadoop.security.authentication");
        }

        private static boolean isKerberosAuth(HdfsConnection hdfsConnection) {
            return StringUtils.equalsIgnoreCase("kerberos", hdfsConnection.getConfigurationEntries()
                    .get("hadoop.security.authentication"));
        }

        private static boolean isKeytabProvided(HdfsConnection hdfsConnection) {
            return hdfsConnection.getAdditionalData()
                    .containsKey("hadoop.security.kerberos.keytab");
        }
    }

}
