///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.connection.config;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.mule.api.ConnectionException;
//import org.mule.api.annotations.Configurable;
//import org.mule.api.annotations.Connect;
//import org.mule.api.annotations.TestConnectivity;
//import org.mule.api.annotations.components.ConnectionManagement;
//import org.mule.api.annotations.display.FriendlyName;
//import org.mule.api.annotations.display.Placement;
//import org.mule.api.annotations.param.ConnectionKey;
//import org.mule.api.annotations.param.Optional;
//
///**
// * Simple authentication configuration. Here you can configure properties required by "Simple Authentication" in order to establish connection with Hadoop Distributed File System.
// *
// * @author MuleSoft, Inc.
// */
//@ConnectionManagement(friendlyName = "Simple Configuration")
//public class Simple extends AbstractConfig {
//
//    private HadoopClientConfigurationProvider hadoopClientConfigurationProvider;
//
//    /**
//     * User identity that Hadoop uses for permissions in HDFS. <br/>
//     * When Simple Authentication is used, Hadoop requires the user to be set as a System Property called HADOOP_USER_NAME. If you fill this field then the connector will set it
//     * for you, however you can set it by yourself. If the variable is not set, Hadoop will use the current logged in OS user.
//     */
//    @Configurable
//    @Optional
//    @Placement(order = 1, group = "Authentication")
//    private String username;
//
//    private String principal;
//
//    public String getUsername() {
//        return username;
//    }
//
//    public void setUsername(String username) {
//        this.username = username;
//    }
//
//    /**
//     * Establish the connection to the Hadoop Distributed File System.
//     *
//     * @param nameNodeUri
//     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
//     *            in configurationResources and configurationEntries.
//     * @throws org.mule.api.ConnectionException
//     *             Holding information regarding reason of failure while trying to connect to the system.
//     */
//    @Connect
//    @TestConnectivity
//    public void connect(@ConnectionKey @FriendlyName("NameNode URI") final String nameNodeUri) throws ConnectionException {
//        putProvidedUsernameAsSystemProperty();
//        hadoopClientConfigurationProvider = new HadoopClientConfigurationProvider();
//        final Configuration configuration = hadoopClientConfigurationProvider.forSimpleAuth(nameNodeUri, getUsername(), getConfigurationResources(), getConfigurationEntries());
//        UserGroupInformation.setConfiguration(configuration);
//        fileSystem(configuration);
//    }
//
//    private void putProvidedUsernameAsSystemProperty() {
//        if (StringUtils.isNotEmpty(getUsername())) {
//            System.setProperty("HADOOP_USER_NAME", getUsername());
//        }
//    }
//}
