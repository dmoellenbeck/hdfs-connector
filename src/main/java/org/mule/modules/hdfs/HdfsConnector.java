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
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.MuleRuntimeException;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.InvalidateConnectionOn;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.util.CollectionUtils;
import org.mule.util.IOUtils;
import org.mule.util.MapUtils;
import org.mule.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Hadoop Distributed File System (HDFS) Connector.
 * </p>
 * 
 * @author MuleSoft, Inc.
 */
@Connector(name = "hdfs", schemaVersion = "3.3", friendlyName = "HDFS", minMuleVersion = "3.3.0", description = "HDFS Connector")
// TODO use a relevant category
// @Category(name = "org.mule.tooling.category.security", description = "Security")
public class HdfsConnector
{
    private final static Logger LOGGER = LoggerFactory.getLogger(HdfsConnector.class);

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
     * Establish the connection to the Hadoop Distributed File System.
     * 
     * @param defaultFileSystemName the name of the file system to connect to. It is
     *            passed to HDFS client as the
     *            {@value FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can
     *            be overriden by values in configurationResources and
     *            configurationEntries.
     * @throws ConnectionException Holding one of the possible values in
     *             {@link ConnectionExceptionCode}.
     */
    @Connect
    public void connect(@ConnectionKey @Default(CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT) final String defaultFileSystemName)
        throws ConnectionException
    {
        // FIXME the default value is always passed to defaultFileSystemName!
        final boolean hasConfigurationResources = CollectionUtils.isNotEmpty(configurationResources);

        final Configuration configuration = new Configuration();

        final boolean isDefaultFileSystem = CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT.equals(defaultFileSystemName);

        if (StringUtils.isNotBlank(defaultFileSystemName)
        // avoid overriding values in configurationResources
            && (!hasConfigurationResources || (hasConfigurationResources && !isDefaultFileSystem)))
        {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultFileSystemName);
        }

        if (hasConfigurationResources)
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
     * Disconnects from the Hadoop Distributed File System.
     * 
     * @throws IOException if there is an issue connecting with the file system.
     */
    @Disconnect
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
     * @param bufferSize the buffer size to use when reading the file.
     * @return an {@link InputStream} that contains the file content.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "read")
    @InvalidateConnectionOn(exception = IOException.class)
    public InputStream readFromPath(final String path, @Optional @Default("4096") final int bufferSize)
        throws Exception
    {
        return runHdfsAction(new Callable<InputStream>()
        {
            public InputStream call() throws IOException
            {
                final Path hdfsPath = new Path(path);
                return fileSystem.open(hdfsPath, bufferSize);
            }
        });
    }

    /**
     * Write the current payload to the designated path, either creating a new file
     * or appending to an existing one.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:write-1}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:write-2}
     * 
     * @param path the path of the file to write to.
     * @param permission the file system permission to use if a new file is created.
     * @param overwrite if a pre-existing file should be overwritten with the new
     *            content.
     * @param bufferSize the buffer size to use when appending to the file.
     * @param replication block replication for the file.
     * @param blockSize the buffer size to use when appending to the file.
     * @param payload the payload to append to the file.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "write")
    @InvalidateConnectionOn(exception = IOException.class)
    public void writeToPath(final String path,
                            @Optional @Default("511") final short permission,
                            @Optional @Default("true") final boolean overwrite,
                            @Optional @Default("4096") final int bufferSize,
                            @Optional @Default("1") final short replication,
                            @Optional @Default("4096") final long blockSize,
                            @Payload final InputStream payload) throws Exception
    {
        runHdfsAction(new Callable<Void>()
        {
            public Void call() throws IOException
            {
                final Path hdfsPath = new Path(path);
                final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath, new FsPermission(
                    permission), overwrite, bufferSize, replication, blockSize, null);
                IOUtils.copyLarge(payload, fsDataOutputStream);
                IOUtils.closeQuietly(fsDataOutputStream);
                return null;
            }
        });
    }

    /**
     * Append the current payload to a file located at the designated path.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:append-1}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:append-2}
     * 
     * @param path the path of the file to write to.
     * @param bufferSize the buffer size to use when appending to the file.
     * @param payload the payload to append to the file.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "append")
    @InvalidateConnectionOn(exception = IOException.class)
    public void appendToPath(final String path,
                             @Optional @Default("4096") final int bufferSize,
                             @Payload final InputStream payload) throws Exception
    {
        runHdfsAction(new Callable<Void>()
        {
            public Void call() throws IOException
            {
                final Path hdfsPath = new Path(path);
                final FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsPath, bufferSize);
                IOUtils.copyLarge(payload, fsDataOutputStream);
                IOUtils.closeQuietly(fsDataOutputStream);
                return null;
            }
        });
    }

    // TODO delete

    /**
     * Verifies if a path exists and stops the flow execution if it doesn't.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:exists}
     * 
     * @param path the path whose existence must be checked.
     * @param sourceCallback to invoke the next processor in the chain.
     * @throws Exception if any issue occurs during the execution.
     * @return the result of executing the next message processors if the path
     *         exists, otherwise null.
     */
    @Processor(name = "exists-filter", intercepting = true)
    @InvalidateConnectionOn(exception = IOException.class)
    public Object pathExistsFilter(final String path, final SourceCallback sourceCallback) throws Exception
    {
        return runHdfsAction(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                final Path hdfsPath = new Path(path);
                if (fileSystem.exists(hdfsPath))
                {
                    return sourceCallback.process();
                }
                else
                {
                    return null;
                }
            }
        });
    }

    private <T> T runHdfsAction(final Callable<T> action) throws Exception
    {
        try
        {
            return action.call();
        }
        catch (final FileNotFoundException fnfe)
        {
            // FileNotFoundException being an IOException: rethrow it wrapped with
            // another exception to prevent the connection to be invalidated
            throw new MuleRuntimeException(fnfe);
        }
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
