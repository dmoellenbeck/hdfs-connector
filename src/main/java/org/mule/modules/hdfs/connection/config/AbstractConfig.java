/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.connection.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Abstract configration to be implemented by specific configuration of the connector.
 *
 * @author MuleSoft, Inc.
 */
public abstract class AbstractConfig {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConfig.class);

    /**
     * A simple user identity of a client process. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources
     * and configurationEntries.
     */
    @Configurable
    @Optional
    @Placement(order = 1, group = "Authentication")
    private String username;
    /**
     * A {@link java.util.List} of configuration resource files to be loaded by the HDFS client. Here you can provide additional configuration files. (e.g core-site.xml)
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private List<String> configurationResources;
    /**
     * A {@link java.util.Map} of configuration entries to be used by the HDFS client. Here you can provide additional configuration entries as key/value pairs.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private Map<String, String> configurationEntries;
    private FileSystem fileSystem;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
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

    protected void fileSystem(Configuration configuration) throws ConnectionException {
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
}
