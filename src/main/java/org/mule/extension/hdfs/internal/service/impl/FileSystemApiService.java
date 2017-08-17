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
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.dto.FileStatusDTO;
import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.HdfsException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;

public class FileSystemApiService implements HdfsAPIService {

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
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
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
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatusDTO> listStatus(String path, String filter)
            throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            if (StringUtils.isNotEmpty(filter)) {
                PathFilter pathFilter = getPathFilter(hdfsPath, filter);
                return mapFileStatusFiles(fileSystem.listStatus(hdfsPath, pathFilter));
            }
            return mapFileStatusFiles(fileSystem.listStatus(hdfsPath));

        } catch (ConnectException e) {
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException("Invalid input filter: " + e.getMessage(), e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException("Invalid file path: " + e.getMessage(), e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
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
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (PatternSyntaxException e) {
            throw new InvalidRequestDataException("Invalid input filter: " + e.getMessage(), e.getMessage(), e);
        } catch (FileNotFoundException e) {
            throw new InvalidRequestDataException("Invalid file path: " + e.getMessage(), e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
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
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
        }
    }
    
    @Override
    public void deleteDirectory(String path) throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException {
        try {
            Path hdfsPath = new Path(path);
            boolean result = fileSystem.delete(hdfsPath, true);
            if (!result) {
                throw new HdfsException("Unable to delete directory!");
            }
        } catch (ConnectException e) {
            throw new HdfsConnectionException("Something went wrong while sending data to hadoop: " + e.getMessage(), e);
        } catch (IllegalArgumentException | IOException e) {
            throw new InvalidRequestDataException("Something went wrong with input data: " + e.getMessage(), e.getMessage(), e);
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
