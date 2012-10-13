/*
 * $Id: LicenseManager.java 10480 2007-12-19 00:47:04Z moosa $
 * --------------------------------------------------------------------------------------
 * (c) 2003-2008 MuleSource, Inc. This software is protected under international copyright
 * law. All use of this software is subject to MuleSource's Master Subscription Agreement
 * (or other master license agreement) separately entered into in writing between you and
 * MuleSource. If such an agreement is not in place, you may not use the software.
 */

package org.mule.modules.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.MuleRuntimeException;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.InvalidateConnectionOn;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.Optional;
import org.mule.util.CollectionUtils;
import org.mule.util.MapUtils;
import org.mule.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Hadoop File System (HDFS) Connector.
 * </p>
 * 
 * @author MuleSoft, Inc.
 */
@Connector(name = "hdfs", schemaVersion = "3.3", friendlyName = "HDFS", minMuleVersion = "3.3.0", description = "HDFS Connector")
// TODO use a relevant category
// @Category(name = "org.mule.tooling.category.security", description = "Security")
public class HdfsModule
{
    private final static Logger LOGGER = LoggerFactory.getLogger(HdfsModule.class);

    /**
     * Default name of the file system to connect to. Passed to HDFS client as the
     * {@value FileSystem#FS_DEFAULT_NAME_KEY} configuration entry.
     */
    @Configurable
    @Optional
    private String defaultFileSystemName;

    /**
     * A {@link List} of configuration resource files to be loaded by the HDFS
     * client.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private List<String> configurationResources;

    /**
     * A {@link Map} of configuration entries to be used by the HDFS client.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private Map<String, String> configurationEntries;

    private FileSystem fileSystem;

    /**
     * Establish the connection to the Hadoop File System.
     * 
     * @throws ConnectionException Holding one of the possible values in
     *             {@link ConnectionExceptionCode}.
     */
    @Connect
    public void connect() throws ConnectionException
    {
        final Configuration configuration = new Configuration();

        if (StringUtils.isNotBlank(defaultFileSystemName))
        {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultFileSystemName);
        }

        if (CollectionUtils.isNotEmpty(configurationResources))
        {
            for (final String configurationResource : configurationResources)
            {
                configuration.addResource(new Path(configurationResource));
            }
        }

        if (MapUtils.isNotEmpty(configurationEntries))
        {
            for (final Entry<String, String> configurationEntry : configurationEntries.entrySet())
            {
                configuration.set(configurationEntry.getKey(), configurationEntry.getValue());
            }
        }

        try
        {
            fileSystem = FileSystem.get(configuration);
        }
        catch (final IOException ioe)
        {
            throw new ConnectionException(ConnectionExceptionCode.CANNOT_REACH, null, ioe.getMessage(), ioe);
        }

        LOGGER.info("Connected to: " + getFileSystemUri());
    }

    @ConnectionIdentifier
    public String getFileSystemUri()
    {
        return fileSystem == null ? null : fileSystem.getUri().toString();
    }

    /**
     * Are we connected?
     * 
     * @return boolean <i>true</i> if the connection is still valid or <i>false</i>
     *         otherwise.
     */
    @ValidateConnection
    public boolean isConnected()
    {
        try
        {
            if (fileSystem != null)
            {
                // ignore the result: an exception will be thrown in case of issue
                fileSystem.listStatus(new Path("/"));
                return true;
            }
        }
        catch (final IOException ioe)
        {
            LOGGER.error("Failed to connect to HDFS", ioe);
        }
        return false;
    }

    /**
     * Disconnects from the Hadoop File System.
     * 
     * @throws IOException if there is an issue connecting with the file system.
     */
    // FIXME figure out why DevKit crashes if this is uncommented
    // @Disconnect
    public void disconnect() throws IOException
    {
        if (fileSystem != null)
        {
            try
            {
                fileSystem.close();
            }
            finally
            {
                fileSystem = null;
            }
        }
    }

    /**
     * Reads the content of a file designated by its path.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:read-1}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:read-2}
     * 
     * @param path the path of the file to read.
     * @param bufferSize an optional buffer size to be used when reading the file.
     * @return an {@link InputStream} that contains the file content.
     * @throws IOException if there is an issue connecting with the file system.
     */
    @Processor
    @InvalidateConnectionOn(exception = IOException.class)
    public InputStream read(final String path, @Optional final Integer bufferSize) throws IOException
    {
        try
        {
            final Path hdfsPath = new Path("/tmp/deptree");
            return bufferSize == null ? fileSystem.open(hdfsPath) : fileSystem.open(hdfsPath, bufferSize);
        }
        catch (final FileNotFoundException fnfe)
        {
            // FileNotFoundException being an IOException: rethrow it wrapped with
            // another exception to prevent the connection to be invalidated
            throw new MuleRuntimeException(fnfe);
        }
    }

    public String getDefaultFileSystemName()
    {
        return defaultFileSystemName;
    }

    public void setDefaultFileSystemName(final String defaultFileSystemName)
    {
        this.defaultFileSystemName = defaultFileSystemName;
    }

    public List<String> getConfigurationResources()
    {
        return configurationResources;
    }

    public void setConfigurationResources(final List<String> configurationResources)
    {
        this.configurationResources = configurationResources;
    }

    public Map<String, String> getConfigurationEntries()
    {
        return configurationEntries;
    }

    public void setConfigurationEntries(final Map<String, String> configurationEntries)
    {
        this.configurationEntries = configurationEntries;
    }
}
