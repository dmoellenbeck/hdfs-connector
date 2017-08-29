/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.operation;

import static org.mule.extension.hdfs.internal.mapping.factory.MapperFactory.dozerMapper;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.api.error.HdfsErrorType;
import org.mule.extension.hdfs.api.error.HdfsOperationErrorTypeProvider;
import org.mule.extension.hdfs.api.operation.param.WriteOpParams;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.dto.FileStatusDTO;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;
import org.mule.extension.hdfs.internal.service.factory.ServiceFactory;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.exception.ModuleException;

public class HdfsOperations {

    private ServiceFactory serviceFactory = new ServiceFactory();

    /**
     * Read the content of a file designated by its path and streams it to the rest of the flow:
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file to read.
     * @param bufferSize
     *            the buffer size to use when reading the file.
     * @return the result from executing the rest of the flow.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public InputStream readOperation(@Connection HdfsConnection connection,
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
     * @param connection
     *            the connection object
     * @param param
     *            parameters used for write operation
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void write(@Connection HdfsConnection connection,
            @ParameterGroup(name = "Input parameters") WriteOpParams param) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.create(param.getPath(), param.getPermission(), param.isOverwrite(), param.getBufferSize(), param.getReplication(),
                    param.getBlockSize(), param.getOwnerUserName(), param.getOwnerGroupName(), param.getPayload());
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
     * List the statuses of the files/directories in the given path if the path is a directory
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the given path
     * @param filter
     *            the user supplied path filter
     * @return the statuses of the files/directories in the given path
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public List<FileStatus> listStatus(@Connection HdfsConnection connection, final String path, @Optional final String filter) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            List<FileStatusDTO> flsDtos = hdfsApiService.listStatus(path, filter);
            BeanMapper beanMapper = dozerMapper();
            return flsDtos.stream()
                    .map(flsDto -> beanMapper.map(flsDto, FileStatus.class))
                    .collect(Collectors.toList());
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
     * Return all the files that match file pattern and are not checksum files. Results are sorted by their names.
     * 
     * @param connection
     *            the connection object
     * @param pathPattern
     *            a regular expression specifying the path pattern.
     * @param filter
     *            the user supplied path filter
     * @return an array of paths that match the path pattern.
     */
    public List<FileStatus> globStatus(@Connection HdfsConnection connection, String pathPattern, @Optional String filter) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {

            List<FileStatusDTO> flsDtos = hdfsApiService.globStatus(pathPattern, filter);
            BeanMapper beanMapper = dozerMapper();
            return flsDtos.stream()
                    .map(flsDto -> beanMapper.map(flsDto, FileStatus.class))
                    .collect(Collectors.toList());
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
     * Make the given file and all non-existent parents into directories. Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path to create directories for.
     * @param permission
     *            the file system permission to use when creating the directories, either in octal or symbolic format (umask).
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void makeDirectories(@Connection HdfsConnection connection,
            String path, @Optional final String permission) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.mkdirs(path, permission);
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
     * Delete the file or directory located at the designated path.
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file to delete.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void deleteDirectory(@Connection HdfsConnection connection,
            String path) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.deleteDirectory(path);
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
     * Append the current payload to a file located at the designated path. <b>Note:</b> by default the Hadoop server has the append option disabled. In order to be able append any
     * data to an existing file refer to dfs.support.append configuration parameter
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file to write to.
     * @param bufferSize
     *            the buffer size to use when appending to the file.
     * @param payload
     *            the payload to append to the file.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void append(@Connection HdfsConnection connection,
            final String path, @Optional(defaultValue = "4096") final int bufferSize, @Content InputStream payload) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.append(path, bufferSize, payload);
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
     * Delete the file or directory located at the designated path.
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file to delete.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void deleteFile(@Connection HdfsConnection connection,
            String path) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.deleteFile(path);
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
