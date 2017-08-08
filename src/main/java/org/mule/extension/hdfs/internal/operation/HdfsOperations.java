/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.operation;

import java.io.InputStream;

import org.mule.extension.hdfs.api.error.HdfsErrorType;
import org.mule.extension.hdfs.api.error.HdfsOperationErrorTypeProvider;
import org.mule.extension.hdfs.internal.config.HdfsConfiguration;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;
import org.mule.extension.hdfs.internal.service.factory.ServiceFactory;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.exception.ModuleException;

public class HdfsOperations {

    private ServiceFactory serviceFactory = new ServiceFactory();

    /**
     * Read the content of a file designated by its path and streams it to the rest of the flow:
     *
     * @param path
     *            the path of the file to read.
     * @param bufferSize
     *            the buffer size to use when reading the file.
     * @return the result from executing the rest of the flow.
     * @throws HDFSConnectorException
     *             if any issue occurs during the execution.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public InputStream readOperation(
            @Config HdfsConfiguration configuration,
            @Connection HdfsConnection connection,
            String path,
            @Optional(defaultValue = "4096") final int bufferSize)

    {
        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            return hdfsApiService.read(path, bufferSize);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Write the current payload to the designated path, either creating a new file or appending to an existing one.
     *
     * @param path
     *            the path of the file to write to.
     * @param permission
     *            the file system permission to use if a new file is created, either in octal or symbolic format (umask).
     * @param overwrite
     *            if a pre-existing file should be overwritten with the new content.
     * @param bufferSize
     *            the buffer size to use when appending to the file.
     * @param replication
     *            block replication for the file.
     * @param blockSize
     *            the buffer size to use when appending to the file.
     * @param ownerUserName
     *            the username owner of the file.
     * @param ownerGroupName
     *            the group owner of the file.
     * @param payload
     *            the payload to write to the file.
     * @throws HDFSConnectorException
     *             if any issue occurs during the execution.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void write(
            @Config HdfsConfiguration configuration,
            @Connection HdfsConnection connection,
            String path,
            @Optional(defaultValue = "700") String permission,
            @Optional(defaultValue = "true") boolean overwrite,
            @Optional(defaultValue = "4096") int bufferSize,
            @Optional(defaultValue = "1") int replication,
            @Optional(defaultValue = "1048576") long blockSize,
            @Optional final String ownerUserName,
            @Optional final String ownerGroupName,
            @Content InputStream payload) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.create(path, permission, overwrite, bufferSize, replication,
                    blockSize, ownerUserName, ownerGroupName, payload);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

}
