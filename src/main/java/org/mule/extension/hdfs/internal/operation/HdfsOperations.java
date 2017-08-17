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
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.exception.ModuleException;

public class HdfsOperations {

    private ServiceFactory serviceFactory = new ServiceFactory();

    /**
     * Read the content of a file designated by its path and streams it to the rest of the flow:
     * 
     * @param configuration
     * @param connection
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
     * @param configuration
     * @param connection
     * @param param
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
     * @param configuration
     * @param connection
     *
     * @param path
     *            the given path
     * @param filter
     *            the user supplied path filter
     * @return FileStatus the statuses of the files/directories in the given path
     * @return
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
     * @param configuration
     * @param connection
     * @param pathPattern
     *            a regular expression specifying the path pattern.
     * @param filter
     *            the user supplied path filter
     * @return FileStatus an array of paths that match the path pattern.
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
     * @param path
     *            the path to create directories for.
     * @param permission
     *            the file system permission to use when creating the directories, either in octal or symbolic format (umask).
     * @throws HDFSConnectorException
     *             if any issue occurs during the execution.
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
     * @param configuration
     * @param connection
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

}
