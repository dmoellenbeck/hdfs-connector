/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.operation;

public class HDFSConnectorOperations {

    // /**
    // * Read the content of a file designated by its path and streams it to the rest of the flow:
    // *
    // * @param path
    // * the path of the file to read.
    // * @param bufferSize
    // * the buffer size to use when reading the file.
    // */
    // public InputStream readOperation(@Connection AbstractConfig connection, String path, @Optional(defaultValue = "4096") int bufferSize) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // InputStream result = connector.readOperation(path, bufferSize);
    // return result;
    // }
    //
    // /**
    // * Get the metadata of a path, as described in {@link HDFSConnector#read(String, int, SourceCallback)}, and store it in flow variables.
    // * <p>
    // * This flow variables are:
    // * <ul>
    // * <li>hdfs.path.exists - Indicates if the path exists (true or false)</li>
    // * <li>hdfs.content.summary - A resume of the path info</li>
    // * <li>hdfs.file.checksum - MD5 digest of the file (if it is a file and exists)</li>
    // * <li>hdfs.file.status - A Hadoop object that contains info about the status of the file (org.apache.hadoop.fs.FileStatus</li>
    // * </ul>
    // *
    // * @param muleEvent
    // * the {@link MuleEvent} currently being processed.
    // * @param path
    // * the path whose existence must be checked.
    // */
    // public void getMetadata(@Connection AbstractConfig connection, String path) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // // connector.getMetadata(path, muleEvent);
    // }
    //
    // /**
    // * Write the current payload to the designated path, either creating a new file or appending to an existing one.
    // *
    // * @param ownerUserName
    // * the username owner of the file.
    // * @param overwrite
    // * if a pre-existing file should be overwritten with the new content.
    // * @param payload
    // * the payload to write to the file.
    // * @param bufferSize
    // * the buffer size to use when appending to the file.
    // * @param ownerGroupName
    // * the group owner of the file.
    // * @param path
    // * the path of the file to write to.
    // * @param permission
    // * the file system permission to use if a new file is created, either in octal or symbolic format (umask).
    // * @param blockSize
    // * the buffer size to use when appending to the file.
    // * @param replication
    // * block replication for the file.
    // */
    // public void write(@Connection AbstractConfig connection, String path, @Optional(defaultValue = "700") String permission, @Optional(defaultValue = "true") boolean overwrite,
    // @Optional(defaultValue = "4096") int bufferSize, @Optional(defaultValue = "1") int replication, @Optional(defaultValue = "1048576") long blockSize,
    // @Optional String ownerUserName, @Optional String ownerGroupName, @Content InputStream payload) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.write(path, permission, overwrite, bufferSize, replication, blockSize, ownerUserName, ownerGroupName, payload);
    // }
    //
    // /**
    // * Append the current payload to a file located at the designated path. <b>Note:</b> by default the Hadoop server has the append option disabled. In order to be able append
    // any
    // * data to an existing file refer to dfs.support.append configuration parameter
    // *
    // * @param bufferSize
    // * the buffer size to use when appending to the file.
    // * @param path
    // * the path of the file to write to.
    // * @param payload
    // * the payload to append to the file.
    // */
    // public void append(@Connection AbstractConfig connection, String path, @Optional(defaultValue = "4096") int bufferSize, @Content InputStream payload)
    // throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.append(path, bufferSize, payload);
    // }
    //
    // /**
    // * Delete the file or directory located at the designated path.
    // *
    // * @param path
    // * the path of the file to delete.
    // */
    // public void deleteFile(@Connection AbstractConfig connection, String path) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.deleteFile(path);
    // }
    //
    // /**
    // * Delete the file or directory located at the designated path.
    // *
    // * @param path
    // * the path of the directory to delete.
    // */
    // public void deleteDirectory(@Connection AbstractConfig connection, String path) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.deleteDirectory(path);
    // }
    //
    // /**
    // * Make the given file and all non-existent parents into directories. Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    // *
    // * @param path
    // * the path to create directories for.
    // * @param permission
    // * the file system permission to use when creating the directories, either in octal or symbolic format (umask).
    // */
    // public void makeDirectories(@Connection AbstractConfig connection, String path, @Optional String permission) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.makeDirectories(path, permission);
    // }
    //
    // /**
    // * Renames path target to path destination.
    // *
    // * @param source
    // * the source path to be renamed.
    // * @param target
    // * the target new path after rename.
    // */
    // // public Boolean rename(@Connection AbstractConfig connection, String source, String target) throws HDFSConnectorException {
    // // HDFSConnector connector = new HDFSConnector();
    // // connector.setConnection(connection);
    // // Boolean result = connector.rename(source, target);
    // // return result;
    // // }
    //
    // /**
    // * List the statuses of the files/directories in the given path if the path is a directory
    // *
    // * @param filter
    // * the user supplied path filter
    // * @param path
    // * the given path
    // */
    // public List<FileStatus> listStatus(@Connection AbstractConfig connection, String path, @Optional String filter) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // List<FileStatus> result = connector.listStatus(path, filter);
    // return result;
    // }
    //
    // /**
    // * Return all the files that match file pattern and are not checksum files. Results are sorted by their names.
    // *
    // * @param filter
    // * the user supplied path filter
    // * @param pathPattern
    // * a regular expression specifying the path pattern.
    // */
    // public List<FileStatus> globStatus(@Connection AbstractConfig connection, String pathPattern, @Optional PathFilter filter) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // List<FileStatus> result = connector.globStatus(pathPattern, filter);
    // return result;
    // }
    //
    // /**
    // * Copy the source file on the local disk to the FileSystem at the given target path, set deleteSource if the source should be removed.
    // *
    // * @param source
    // * the source path on the local disk.
    // * @param deleteSource
    // * whether to delete the source.
    // * @param overwrite
    // * whether to overwrite a existing file.
    // * @param target
    // * the target path on the File System.
    // */
    // // public void copyFromLocalFile(@Connection AbstractConfig connection, @Optional(defaultValue = "false") boolean deleteSource,
    // // @Optional(defaultValue = "true") boolean overwrite, String source, String target) throws HDFSConnectorException {
    // // HDFSConnector connector = new HDFSConnector();
    // // connector.setConnection(connection);
    // // connector.copyFromLocalFile(deleteSource, overwrite, source, target);
    // // }
    //
    // /**
    // * Copy the source file on the FileSystem to local disk at the given target path, set deleteSource if the source should be removed. useRawLocalFileSystem indicates whether to
    // * use RawLocalFileSystem as it is a non CRC File System.
    // *
    // * @param useRawLocalFileSystem
    // * whether to use RawLocalFileSystem as local file system or not.
    // * @param deleteSource
    // * whether to delete the source.
    // * @param source
    // * the source path on the File System.
    // * @param target
    // * the target path on the local disk.
    // */
    // // public void copyToLocalFile(@Connection AbstractConfig connection, @Optional(defaultValue = "false") boolean deleteSource,
    // // @Optional(defaultValue = "false") boolean useRawLocalFileSystem, String source, String target) throws HDFSConnectorException {
    // // HDFSConnector connector = new HDFSConnector();
    // // connector.setConnection(connection);
    // // connector.copyToLocalFile(deleteSource, useRawLocalFileSystem, source, target);
    // // }
    //
    // /**
    // * Set permission of a path (i.e., a file or a directory).
    // *
    // * @param permission
    // * the file system permission to be set.
    // * @param path
    // * the path of the file or directory to set permission.
    // */
    // public void setPermission(@Connection AbstractConfig connection, String path, String permission) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.setPermission(path, permission);
    // }
    //
    // /**
    // * Set owner of a path (i.e., a file or a directory). The parameters username and groupname cannot both be null.
    // *
    // * @param path
    // * the path of the file or directory to set owner.
    // * @param ownername
    // * If it is null, the original username remains unchanged.
    // * @param groupname
    // * If it is null, the original groupname remains unchanged.
    // */
    // public void setOwner(@Connection AbstractConfig connection, String path, @Optional String ownername, @Optional String groupname) throws HDFSConnectorException {
    // HDFSConnector connector = new HDFSConnector();
    // connector.setConnection(connection);
    // connector.setOwner(path, ownername, groupname);
    // }

}
