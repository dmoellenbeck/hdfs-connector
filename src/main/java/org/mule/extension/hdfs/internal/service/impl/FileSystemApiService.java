/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.impl;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.dto.FileStatusDTO;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.HdfsException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;

public class FileSystemApiService implements HdfsAPIService {

    public static final String HDFS = "hdfs";
    public static final String HDFS_PATH_EXISTS = HDFS + ".path.exists";
    public static final String HDFS_FILE_STATUS = HDFS + ".file.status";
    public static final String HDFS_FILE_CHECKSUM = HDFS + ".file.checksum";
    public static final String HDFS_CONTENT_SUMMARY = HDFS + ".content.summary";

    private FileSystem fileSystem;
    private BeanMapper beanMapper;

    public FileSystemApiService(FileSystemConnection fsConn, BeanMapper beanMapper) {
        this.fileSystem = fsConn.getFileSystem();
        this.beanMapper = beanMapper;
    }

    @Override
    public InputStream read(String path, int bufferSize)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            InputStream input = fileSystem.open(hdfsPath, bufferSize);
            return IOUtils.toBufferedInputStream(input);
        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void create(String path, String permission, boolean overwrite, int bufferSize, int replication, long blockSize, String ownerUserName, String ownerGroupName,
            InputStream payload)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {

        try {

            validateCreate(payload);

            Path hdfsPath = new Path(path);
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath, getFileSystemPermission(permission), overwrite, bufferSize, (short) replication,
                    blockSize, null);
            IOUtils.copyLarge(payload, fsDataOutputStream);
            fsDataOutputStream.hsync();
            IOUtils.closeQuietly(fsDataOutputStream);

            if ((isNotBlank(ownerUserName)) || (isNotBlank(ownerGroupName))) {
                fileSystem.setOwner(hdfsPath, ownerUserName, ownerGroupName);
            }

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatusDTO> listStatus(String path, String filter)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            if (StringUtils.isNotBlank(filter)) {
                PathFilter pathFilter = getPathFilter(hdfsPath, filter);
                return mapFileStatusFiles(fileSystem.listStatus(hdfsPath, pathFilter));
            }
            return mapFileStatusFiles(fileSystem.listStatus(hdfsPath));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(PatternSyntaxException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(FileNotFoundException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatusDTO> globStatus(String pathPattern, String filter)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {

        try {
            Path hdfsPath = new Path(pathPattern);
            if (StringUtils.isNotEmpty(filter)) {
                PathFilter pathFilter = getPathFilter(hdfsPath, filter);
                return mapFileStatusFiles(fileSystem.globStatus(hdfsPath, pathFilter));
            }

            return mapFileStatusFiles(fileSystem.globStatus(hdfsPath));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(PatternSyntaxException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(FileNotFoundException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void mkdirs(String path, String permission)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {

        try {

            FsPermission fsPermission = getFileSystemPermission(permission);
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.mkdirs(hdfsPath, fsPermission);
            if (!result) {
                throw new HdfsException("Unable to create directory!");
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void deleteDirectory(String path) throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.delete(hdfsPath, true);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_DELETE_DIR);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void append(String path, int bufferSize, InputStream payload)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {

            validateCreate(payload);

            Path hdfsPath = new Path(path);

            final FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsPath, bufferSize);
            IOUtils.copyLarge(payload, fsDataOutputStream);
            IOUtils.closeQuietly(fsDataOutputStream);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }

    }

    @Override
    public void deleteFile(String path) throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.delete(hdfsPath, false);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_DELETE_FILE);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> getMetadata(String path) {
        Path hdfsPath = new Path(path);
        final Map<String, Object> metaData = new HashMap<>();

        try {
            final boolean pathExists = fileSystem.exists(hdfsPath);
            metaData.put(HDFS_PATH_EXISTS, pathExists);
            if (!pathExists) {
                return metaData;
            }

            metaData.put(HDFS_CONTENT_SUMMARY, fileSystem.getContentSummary(hdfsPath));

            final FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
            metaData.put(HDFS_FILE_STATUS, fileStatus);
            if (fileStatus.isDirectory()) {
                return metaData;
            }

            final FileChecksum fileChecksum = fileSystem.getFileChecksum(hdfsPath);
            if (fileChecksum != null) {
                metaData.put(HDFS_FILE_CHECKSUM, fileChecksum);
            }
        } catch (IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
        return metaData;
    }

    @Override
    public void rename(String source, String destination)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            boolean result = fileSystem.rename(hdfsSourcePath, hdfsDestPath);
            if (!result) {
                throw new HdfsException(ExceptionMessages.UNABLE_TO_RENAME_PATH);
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }

    }

    @Override
    public void copyToLocalFile(boolean deleteSource, boolean useRawLocalFileSystem, String source, String destination)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {

        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            fileSystem.copyToLocalFile(deleteSource, hdfsSourcePath, hdfsDestPath, useRawLocalFileSystem);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void copyFromLocalFile(boolean deleteSource, boolean overwrite, String source, String destination)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {

        try {
            Path hdfsSourcePath = new Path(source);
            Path hdfsDestPath = new Path(destination);
            fileSystem.copyFromLocalFile(deleteSource, overwrite, hdfsSourcePath, hdfsDestPath);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void setPermission(String path, String permission)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            fileSystem.setPermission(hdfsPath, getFileSystemPermission(permission));

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void setOwner(String path, String ownername, String groupname)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            fileSystem.setOwner(hdfsPath, ownername, groupname);

        } catch (ConnectException e) {
            throw new HdfsConnectionException(ExceptionMessages.resolveExceptionMessage(HdfsConnectionException.class.getSimpleName()) + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException(ExceptionMessages.resolveExceptionMessage(InvalidRequestDataException.class.getSimpleName()) + e.getMessage(), e.getMessage(), e);
        }
    }

    private List<FileStatusDTO> mapFileStatusFiles(FileStatus[] files) {
        if (files != null) {
            return Arrays.stream(files)
                    .map(fs -> beanMapper.map(fs, FileStatusDTO.class))
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
