/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.impl;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.extension.hdfs.api.CheckSummary;
import org.mule.extension.hdfs.api.ContentSummary;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.api.MetaData;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.HdfsException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class FileSystemApiService implements HdfsAPIService {

    public static final String HDFS = "hdfs";

    private FileSystem fileSystem;
    private BeanMapper beanMapper;

    public FileSystemApiService(FileSystemConnection fsConn, BeanMapper beanMapper) {
        this.fileSystem = fsConn.getFileSystem();
        this.beanMapper = beanMapper;
    }

    @Override
    public InputStream read(String path, int bufferSize) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            InputStream input = fileSystem.open(hdfsPath, bufferSize);
            return IOUtils.toBufferedInputStream(input);
        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void create(String path, String permission, boolean overwrite, int bufferSize, int replication,
            long blockSize, String ownerUserName, String ownerGroupName, InputStream payload)
            throws InvalidRequestDataException,
            HdfsConnectionException {

        try {

            validateCreate(payload);

            Path hdfsPath = new Path(path);
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath,
                    getFileSystemPermission(permission), overwrite, bufferSize, (short) replication, blockSize, null);
            IOUtils.copyLarge(payload, fsDataOutputStream);
            fsDataOutputStream.hsync();
            IOUtils.closeQuietly(fsDataOutputStream);

            if ((isNotBlank(ownerUserName)) || (isNotBlank(ownerGroupName))) {
                fileSystem.setOwner(hdfsPath, ownerUserName, ownerGroupName);
            }

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatus> listStatus(String path, String filter) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            if (StringUtils.isNotBlank(filter)) {
                PathFilter pathFilter = getPathFilter(hdfsPath, filter);
                return mapFileStatusFiles(fileSystem.listStatus(hdfsPath, pathFilter));
            }
            return mapFileStatusFiles(fileSystem.listStatus(hdfsPath));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(PatternSyntaxException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(FileNotFoundException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatus> globStatus(String pathPattern, String filter) throws InvalidRequestDataException,
            HdfsConnectionException {

        try {
            Path hdfsPath = new Path(pathPattern);
            if (StringUtils.isNotEmpty(filter)) {
                PathFilter pathFilter = getPathFilter(hdfsPath, filter);
                return mapFileStatusFiles(fileSystem.globStatus(hdfsPath, pathFilter));
            }

            return mapFileStatusFiles(fileSystem.globStatus(hdfsPath));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(PatternSyntaxException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(FileNotFoundException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void mkdirs(String path, String permission) throws InvalidRequestDataException,
            HdfsConnectionException {

        try {

            FsPermission fsPermission = getFileSystemPermission(permission);
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.mkdirs(hdfsPath, fsPermission);
            if (!result) {
                throw new HdfsException("Unable to create directory!");
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void deleteDirectory(String path) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.delete(hdfsPath, true);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_DELETE_DIR);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void append(String path, int bufferSize, InputStream payload) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {

            validateCreate(payload);

            Path hdfsPath = new Path(path);

            final FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsPath, bufferSize);
            IOUtils.copyLarge(payload, fsDataOutputStream);
            IOUtils.closeQuietly(fsDataOutputStream);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }

    }

    @Override
    public void deleteFile(String path) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.delete(hdfsPath, false);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_DELETE_FILE);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public MetaData getMetadata(String path) {
        final MetaData metaData = new MetaData();
        try {
            Path hdfsPath = new Path(path);

            metaData.setPathExists(fileSystem.exists(hdfsPath));

            if (!metaData.isPathExists()) {
                return metaData;
            }

            ContentSummary dto = beanMapper.map(fileSystem.getContentSummary(hdfsPath), ContentSummary.class);

            metaData.setContentSummary(dto);

            final org.apache.hadoop.fs.FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
            metaData.setFileStatus(beanMapper.map(fileStatus, FileStatus.class));

            if (fileStatus.isDirectory()) {
                return metaData;
            }

            final FileChecksum fileChecksum = fileSystem.getFileChecksum(hdfsPath);
            if (fileChecksum != null) {
                CheckSummary checkSummary = beanMapper.map(fileChecksum, CheckSummary.class);
                metaData.setCheckSummary(checkSummary);
            }
        } catch (IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
        return metaData;
    }

    @Override
    public void rename(String source, String destination) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            boolean result = fileSystem.rename(hdfsSourcePath, hdfsDestPath);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_RENAME_PATH);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }

    }

    @Override
    public void copyToLocalFile(boolean deleteSource, boolean useRawLocalFileSystem, String source, String destination)
            throws InvalidRequestDataException,
            HdfsConnectionException {

        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            fileSystem.copyToLocalFile(deleteSource, hdfsSourcePath, hdfsDestPath, useRawLocalFileSystem);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void copyFromLocalFile(boolean deleteSource, boolean overwrite, String source, String destination)
            throws InvalidRequestDataException,
            HdfsConnectionException {

        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            fileSystem.copyFromLocalFile(deleteSource, overwrite, hdfsSourcePath, hdfsDestPath);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void setPermission(String path, String permission) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            fileSystem.setPermission(hdfsPath, getFileSystemPermission(permission));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    @Override
    public void setOwner(String path, String ownername, String groupname) throws InvalidRequestDataException,
            HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            fileSystem.setOwner(hdfsPath, ownername, groupname);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(
                    ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName())
                            + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(
                    ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName())
                            + e.getMessage(),
                    e.getMessage(), e);
        }
    }

    private List<FileStatus> mapFileStatusFiles(org.apache.hadoop.fs.FileStatus[] files) {
        if (files != null) {
            return Arrays.stream(files)
                    .map(fs -> beanMapper.map(fs, FileStatus.class))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private FsPermission getFileSystemPermission(final String permission) {
        return isBlank(permission) ? FsPermission.getDefault() : new FsPermission(permission);
    }

    private void validateCreate(InputStream payload) {
        if (payload == null) {
            throw new InvalidRequestDataException("Payload cannot be null.");
        }
    }

    private boolean isMatching(Path path, Pattern pattern) throws IOException {
        if (fileSystem.getFileStatus(path)
                .isDirectory()) {
            return false;
        } else {
            return pattern.matcher(path.toString())
                    .matches();
        }

    }

    private PathFilter getPathFilter(Path path, String filter) {
        final Pattern pattern = Pattern.compile(filter);
        PathFilter pathFilter = new PathFilter() {

            @Override
            public boolean accept(Path path) {
                try {
                    return isMatching(path, pattern);
                } catch (IOException e) {
                    return false;
                }
            }
        };
        return pathFilter;
    }

}
