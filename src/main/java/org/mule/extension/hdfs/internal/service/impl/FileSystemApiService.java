package org.mule.extension.hdfs.internal.service.impl;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;

public class FileSystemApiService implements HdfsAPIService {

    private FileSystem fileSystem;

    public FileSystemApiService(FileSystemConnection fsConn) {
        this.fileSystem = fsConn.getFileSystem();
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

    private FsPermission getFileSystemPermission(final String permission) {
        return isBlank(permission) ? FsPermission.getDefault() : new FsPermission(permission);
    }

    private void validateCreate(InputStream payload) {
        Objects.requireNonNull(payload, "Payload cannot be null.");
    }

}
