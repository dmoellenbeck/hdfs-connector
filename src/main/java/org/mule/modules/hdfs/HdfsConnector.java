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

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.MuleEvent;
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
public class HdfsConnector
{
    private final static Logger LOGGER = LoggerFactory.getLogger(HdfsConnector.class);

    private interface HdfsPathAction<T>
    {
        T run(Path hdfsPath) throws Exception;
    }

    private interface VoidHdfsPathAction
    {
        void run(Path hdfsPath) throws Exception;
    }

    /**
     * The name of the file system to connect to. It is passed to HDFS client as the
     * {@value FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be
     * overriden by values in configurationResources and configurationEntries.
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

    /**
     * A readily configured {@link FileSystem} to use to connect to HDFS.
     */
    @Configurable
    @Optional
    @Placement(group = "Advanced")
    private FileSystem fileSystem;

    /**
     * Establish the connection to the Hadoop Distributed File System.
     * 
     * @param connectionKey a connection key.
     * @throws ConnectionException Holding one of the possible values in
     *             {@link ConnectionExceptionCode}.
     */
    @Connect
    public void connect(@ConnectionKey @Default("DEFAULT") final String connectionKey)
        throws ConnectionException
    {
        final Configuration configuration = new Configuration();
        if (StringUtils.isNotBlank(defaultFileSystemName))
        {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultFileSystemName);
        }

        final boolean hasConfigurationResources = CollectionUtils.isNotEmpty(configurationResources);
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
        return runHdfsPathAction(path, new HdfsPathAction<InputStream>()
        {
            public InputStream run(final Path hdfsPath) throws Exception
            {
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
     * @param ownerUserName the username owner of the file.
     * @param ownerGroupName the group owner of the file.
     * @param payload the payload to write to the file.
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
                            @Optional final String ownerUserName,
                            @Optional final String ownerGroupName,
                            @Payload final InputStream payload) throws Exception
    {
        runHdfsPathAction(path, new VoidHdfsPathAction()
        {
            public void run(final Path hdfsPath) throws Exception
            {
                final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath, new FsPermission(
                    permission), overwrite, bufferSize, replication, blockSize, null);
                IOUtils.copyLarge(payload, fsDataOutputStream);
                IOUtils.closeQuietly(fsDataOutputStream);

                if ((StringUtils.isNotBlank(ownerUserName)) || (StringUtils.isNotBlank(ownerGroupName)))
                {
                    fileSystem.setOwner(hdfsPath, ownerUserName, ownerGroupName);
                }
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
        runHdfsPathAction(path, new VoidHdfsPathAction()
        {
            public void run(final Path hdfsPath) throws Exception
            {
                final FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsPath, bufferSize);
                IOUtils.copyLarge(payload, fsDataOutputStream);
                IOUtils.closeQuietly(fsDataOutputStream);
            }
        });
    }

    /**
     * Delete the file or directory located at the designated path.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:delete-file}
     * 
     * @param path the path of the file to delete.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "delete-file")
    @InvalidateConnectionOn(exception = IOException.class)
    public void deleteFile(final String path) throws Exception
    {
        deletePath(path, false);
    }

    /**
     * Delete the file or directory located at the designated path.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:delete-directory}
     * 
     * @param path the path of the directory to delete.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "delete-directory")
    @InvalidateConnectionOn(exception = IOException.class)
    public void deleteDirectory(final String path) throws Exception
    {
        deletePath(path, true);
    }

    private void deletePath(final String path, final boolean recursive) throws Exception
    {
        runHdfsPathAction(path, new VoidHdfsPathAction()
        {
            public void run(final Path hdfsPath) throws Exception
            {
                fileSystem.delete(hdfsPath, recursive);
            }
        });
    }

    /**
     * Stores true in a flow variable whose name is provided if the path exists,
     * false otherwise.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample
     * hdfs:set-path-exists-variable}
     * 
     * @param variableName the name of the variable to store the existence boolean
     *            under.
     * @param path the path whose existence must be checked.
     * @param muleEvent the {@link MuleEvent} currently being processed.
     * @throws Exception if any issue occurs during the execution.
     * @return the result of executing the next message processors if the path
     *         exists, otherwise null.
     */
    @Processor(name = "set-path-exists-variable")
    @InvalidateConnectionOn(exception = IOException.class)
    @Inject
    public void setPathExistsVariable(final String variableName, final String path, final MuleEvent muleEvent)
        throws Exception
    {
        runHdfsPathAction(path, new VoidHdfsPathAction()
        {
            public void run(final Path hdfsPath) throws Exception
            {
                muleEvent.setFlowVariable(variableName, fileSystem.exists(hdfsPath));
            }
        });
    }

    /**
     * Make the given file and all non-existent parents into directories. Has the
     * semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an
     * error.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:make-directories-1}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-hdfs.xml.sample hdfs:make-directories-2}
     * 
     * @param path the path to create directories for.
     * @param permission the file system permission to use when creating the
     *            directories.
     * @throws Exception if any issue occurs during the execution.
     */
    @Processor(name = "make-directories")
    @InvalidateConnectionOn(exception = IOException.class)
    public void makeDirectories(final String path, @Optional @Default("511") final short permission)
        throws Exception
    {
        runHdfsPathAction(path, new VoidHdfsPathAction()
        {
            public void run(final Path hdfsPath) throws Exception
            {
                fileSystem.mkdirs(hdfsPath, new FsPermission(permission));
            }
        });
    }

    private void runHdfsPathAction(final String path, final VoidHdfsPathAction action) throws Exception
    {
        runHdfsPathAction(path, new HdfsPathAction<Void>()
        {
            public Void run(final Path hdfsPath) throws Exception
            {
                action.run(hdfsPath);
                return null;
            }
        });
    }

    private <T> T runHdfsPathAction(final String path, final HdfsPathAction<T> action) throws Exception
    {
        try
        {
            final Path hdfsPath = new Path(path);
            return action.run(hdfsPath);
        }
        catch (final FileNotFoundException fnfe)
        {
            // FileNotFoundException being an IOException: rethrow it wrapped with
            // another exception to prevent the connection to be invalidated
            throw new MuleRuntimeException(fnfe);
        }
    }

    public FileSystem getFileSystem()
    {
        return fileSystem;
    }

    public void setFileSystem(final FileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
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
