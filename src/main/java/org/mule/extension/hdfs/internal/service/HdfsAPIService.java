package org.mule.extension.hdfs.internal.service;

import java.io.InputStream;

import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;

public interface HdfsAPIService {

    InputStream read(String path, int bufferSize) throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException;

    void create(String path, String permission, boolean overwrite, int bufferSize, int replication, long blockSize, String ownerUserName, String ownerGroupName,
            InputStream payload) throws InvalidRequestDataException, UnableToRetrieveResponseException, UnableToSendRequestException, HdfsConnectionException;
}
