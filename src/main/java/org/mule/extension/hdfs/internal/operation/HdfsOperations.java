/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.operation;

import static org.mule.extension.hdfs.internal.mapping.factory.MapperFactory.dozerMapper;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.mule.extension.hdfs.api.FileStatus;
import org.mule.extension.hdfs.api.MetaData;
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
import org.mule.runtime.api.metadata.MediaType;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.extension.api.runtime.operation.Result;

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
    public Result<InputStream,Void> readOperation(@Connection HdfsConnection connection, String path,
                                                  @Optional(defaultValue = "4096") final int bufferSize)

    {
        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            return Result.<InputStream,Void>builder().mediaType(MediaType.BINARY).output(hdfsApiService.read(path,bufferSize)).build();
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
            hdfsApiService.create(param.getPath(), param.getPermission(), param.isOverwrite(), param.getBufferSize(),
                    param.getReplication(), param.getBlockSize(), param.getOwnerUserName(), param.getOwnerGroupName(),
                    param.getPayload());
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public List<FileStatus> listStatus(@Connection HdfsConnection connection, final String path,
            @Optional final String filter) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            List<FileStatusDTO> flsDtos = hdfsApiService.listStatus(path, filter);
            BeanMapper beanMapper = dozerMapper();
            return flsDtos.stream()
                    .map(flsDto -> beanMapper.map(flsDto, FileStatus.class))
                    .collect(Collectors.toList());
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public List<FileStatus> globStatus(@Connection HdfsConnection connection, String pathPattern,
            @Optional String filter) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {

            List<FileStatusDTO> flsDtos = hdfsApiService.globStatus(pathPattern, filter);
            BeanMapper beanMapper = dozerMapper();
            return flsDtos.stream()
                    .map(flsDto -> beanMapper.map(flsDto, FileStatus.class))
                    .collect(Collectors.toList());
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public void makeDirectories(@Connection HdfsConnection connection, String path, @Optional final String permission) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.mkdirs(path, permission);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public void deleteDirectory(@Connection HdfsConnection connection, String path) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.deleteDirectory(path);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public void append(@Connection HdfsConnection connection, final String path,
            @Optional(defaultValue = "4096") final int bufferSize, @Content InputStream payload) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.append(path, bufferSize, payload);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
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
    public void deleteFile(@Connection HdfsConnection connection, String path) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.deleteFile(path);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Get the metadata of a path
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file to delete.
     * @return metadata of a path
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public MetaData getMetadata(@Connection HdfsConnection connection, final String path) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            return dozerMapper().map(hdfsApiService.getMetadata(path), MetaData.class);
        } catch (InvalidRequestDataException | IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Renames path target to path destination. *
     * 
     * @param connection
     *            the connection object
     * @param source
     *            the source path to be renamed.
     * @param destination
     *            new path after rename.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void rename(@Connection HdfsConnection connection, String source, final String destination) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.rename(source, destination);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Copy the source file on the FileSystem to local disk at the given target path, set deleteSource if the source should be removed. useRawLocalFileSystem indicates whether to
     * use RawLocalFileSystem as it is a non CRC File System.
     * 
     * @param connection
     *            the connection object
     * @param deleteSource
     *            whether to delete the source.
     * @param useRawLocalFileSystem
     *            whether to use RawLocalFileSystem as local file system or not.
     * @param source
     *            the source path on the File System.
     * @param destination
     *            the target path on the local disk.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void copyToLocalFile(@Connection HdfsConnection connection,
            @Optional(defaultValue = "false") boolean deleteSource,
            @Optional(defaultValue = "false") boolean useRawLocalFileSystem, String source, String destination) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.copyToLocalFile(deleteSource, useRawLocalFileSystem, source, destination);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Copy the source file on the local disk to the FileSystem at the given target path, set deleteSource if the source should be removed.
     * 
     * 
     * @param connection
     *            the connection object
     * @param deleteSource
     *            whether to delete the source.
     * @param overwrite
     *           whether to overwrite destination content.
     * @param source
     *            the source path on the File System.
     * @param destination
     *            the target path on the local disk.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void copyFromLocalFile(@Connection HdfsConnection connection,
            @Optional(defaultValue = "false") boolean deleteSource, @Optional(defaultValue = "true") boolean overwrite,
            String source, String destination) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.copyFromLocalFile(deleteSource, overwrite, source, destination);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Set permission of a path (i.e., a file or a directory).
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file or directory to set permission.
     * @param permission
     *            the file system permission to be set.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void setPermission(@Connection HdfsConnection connection, String path, String permission) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.setPermission(path, permission);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    /**
     * Set owner of a path (i.e., a file or a directory). The parameters username and groupname cannot both be null.
     * 
     * @param connection
     *            the connection object
     * @param path
     *            the path of the file or directory to set owner.
     * @param ownername
     *            If it is null, the original username remains unchanged.
     * @param groupname
     *            If it is null, the original groupname remains unchanged.
     */
    @Throws(HdfsOperationErrorTypeProvider.class)
    public void setOwner(@Connection HdfsConnection connection, String path, String ownername, String groupname) {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        try {
            hdfsApiService.setOwner(path, ownername, groupname);
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + " ErrorCode: " + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

}
