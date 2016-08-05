/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.

 */
/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.connection.strategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.*;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Optional;
import org.mule.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.commons.collections.MapUtils.isNotEmpty;
import static org.apache.commons.lang.StringUtils.isNotBlank;

@ConnectionManagement(friendlyName = "Configuration")
public class HDFSConnectionManagement {

    private static final Logger logger = LoggerFactory.getLogger(HDFSConnectionManagement.class);

    /**
     * A simple user identity of a client process.
     */
    @Configurable
    @Placement(group = "Authentication")
    private String username;

    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private Map<String, String> configurationEntries;

    private FileSystem fileSystem;

    /**
     * Establish the connection to the Hadoop Distributed File System.
     *
     * @param nameNodeUri
     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
     *            in configurationResources and configurationEntries.
     * @throws org.mule.api.ConnectionException
     *             Holding one of the possible values in {@link org.mule.api.ConnectionExceptionCode}.
     */
    @Connect
    @TestConnectivity
    public void connect(@ConnectionKey @FriendlyName("NameNode URI") final String nameNodeUri)
            throws ConnectionException {
        final Configuration configuration = new Configuration();
        if (isNotBlank(nameNodeUri)) {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        }

        if (isNotBlank(username)) {
            System.setProperty("HADOOP_USER_NAME", username);
            configuration.set("hadoop.job.ugi", username);
        }

        final boolean hasConfigurationResources = CollectionUtils.isNotEmpty(configurationResources);
        if (hasConfigurationResources) {
            for (final String configurationResource : configurationResources) {
                configuration.addResource(new Path(configurationResource));
            }
        }

        if (isNotEmpty(configurationEntries)) {
            for (final Map.Entry<String, String> configurationEntry : configurationEntries.entrySet()) {
                configuration.set(configurationEntry.getKey(), configurationEntry.getValue());
            }
        }

        try {
            fileSystem = FileSystem.get(configuration);

            // Tests whether the fileSystem is valid.
            fileSystem.getStatus();

        } catch (final IOException ioe) {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN_HOST, null, ioe.getMessage(), ioe);
        }
        logger.info("Connected to: " + getFileSystemUri());
    }

    /**
     * Returns a prefix used in generating unique identifies for connector instances in the connection pool
     */
    @ConnectionIdentifier
    public String getFileSystemUri() {
        return fileSystem == null ? "hdfs-" : fileSystem.getUri()
                .toString();
    }

    /**
     * Are we connected?
     *
     * @return boolean <i>true</i> if the connection is still valid or <i>false</i> otherwise.
     */
    @ValidateConnection
    public boolean isConnected() {
        try {
            if (fileSystem != null) {
                // ignore the result: an exception will be thrown in case of issue
                fileSystem.listStatus(new Path("/"));
                return true;
            }
        } catch (final IOException ioe) {
            logger.error("Failed to connect to HDFS", ioe);
        }
        return false;
    }

    /**
     * Disconnect from the Hadoop Distributed File System.
     *
     * @throws IOException
     *             if there is an issue connecting with the file system.
     */
    @Disconnect
    public void disconnect() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            } finally {
                fileSystem = null;
            }
        }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(final FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public List<String> getConfigurationResources() {
        return configurationResources;
    }

    public void setConfigurationResources(final List<String> configurationResources) {
        this.configurationResources = configurationResources;
    }

    public Map<String, String> getConfigurationEntries() {
        return configurationEntries;
    }

    public void setConfigurationEntries(final Map<String, String> configurationEntries) {
        this.configurationEntries = configurationEntries;
    }

}
