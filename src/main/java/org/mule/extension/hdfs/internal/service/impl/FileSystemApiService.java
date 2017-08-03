package org.mule.extension.hdfs.internal.service.impl;

import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
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
        // TODO Auto-generated method stub
        return null;
    }

}
