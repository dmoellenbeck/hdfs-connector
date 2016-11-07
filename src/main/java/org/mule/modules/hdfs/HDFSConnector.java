/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs;

/**
 * Hadoop Distributed File System (HDFS) Connector.
 *
 * @author MuleSoft Inc.
 */
public class HDFSConnector {

    // public static final String HDFS = "hdfs";
    // public static final String HDFS_PATH_EXISTS = HDFS + ".path.exists";
    // public static final String HDFS_FILE_STATUS = HDFS + ".file.status";
    // public static final String HDFS_FILE_CHECKSUM = HDFS + ".file.checksum";
    // public static final String HDFS_CONTENT_SUMMARY = HDFS + ".content.summary";
    //
    // private AbstractConfig connection;
    //
    // private FileSystem fileSystem;
    //
    // /**
    // * Read the content of a file designated by its path and streams it to the rest of the flow, while adding the path metadata in the following inbound properties:
    // * <ul>
    // * <li>{@link HDFSConnector#HDFS_PATH_EXISTS}: a boolean set to true if the path exists</li>
    // * <li>{@link HDFSConnector#HDFS_CONTENT_SUMMARY}: an instance of {@link ContentSummary} if the path exists.</li>
    // * <li>{@link HDFSConnector#HDFS_FILE_STATUS}: an instance of {@link FileStatus} if the path exists.</li>
    // * <li>{@link HDFSConnector#HDFS_FILE_CHECKSUM}: an instance of {@link FileChecksum} if the path exists, is a file and has a checksum.</li>
    // * </ul>
    // *
    // * @param path
    // * the path of the file to read.
    // * @param bufferSize
    // * the buffer size to use when reading the file.
    // * @param sourceCallback
    // * the {@link SourceCallback} used to propagate the event to the rest of the flow.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void read(final String path, final int bufferSize, final MessageHandler sourceCallback) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // sourceCallback.handle(
    // Message.builder()
    // .payload(fileSystem.open(hdfsPath, bufferSize))
    // .attributes(fileAttributes(hdfsPath)).build()
    // );
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Read the content of a file designated by its path and streams it to the rest of the flow:
    // *
    // * @param path
    // * the path of the file to read.
    // * @param bufferSize
    // * the buffer size to use when reading the file.
    // * @return the result from executing the rest of the flow.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public InputStream readOperation(final String path, final int bufferSize) throws HDFSConnectorException {
    // try {
    // return runHdfsPathAction(path, new HdfsPathAction<InputStream>() {
    //
    // public InputStream run(final Path hdfsPath) throws Exception { // NOSONAR
    // return fileSystem.open(hdfsPath, bufferSize);
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // private Map<String, Object> getPathMetaData(final Path hdfsPath) throws IOException {
    // final Map<String, Object> metaData = new HashMap<String, Object>();
    //
    // final boolean pathExists = fileSystem.exists(hdfsPath);
    // metaData.put(HDFS_PATH_EXISTS, pathExists);
    // if (!pathExists) {
    // return metaData;
    // }
    //
    // metaData.put(HDFS_CONTENT_SUMMARY, fileSystem.getContentSummary(hdfsPath));
    //
    // final FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
    // metaData.put(HDFS_FILE_STATUS, fileStatus);
    // if (fileStatus.isDirectory()) {
    // return metaData;
    // }
    //
    // final FileChecksum fileChecksum = fileSystem.getFileChecksum(hdfsPath);
    // if (fileChecksum != null) {
    // metaData.put(HDFS_FILE_CHECKSUM, fileChecksum);
    // }
    //
    // return metaData;
    // }
    //
    // private FileAttributes fileAttributes(final Path hdfsPath) throws IOException {
    // Map<String, Object> pathMetadata = getPathMetaData(hdfsPath);
    // return new FileAttributes((boolean)pathMetadata.get(HDFS_PATH_EXISTS),
    // (ContentSummary) pathMetadata.get(HDFS_CONTENT_SUMMARY),
    // (FileStatus) pathMetadata.get(HDFS_FILE_STATUS),
    // (FileChecksum) pathMetadata.get(HDFS_FILE_CHECKSUM));
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
    // * @param path
    // * the path whose existence must be checked.
    // * @param muleEvent
    // * the {@link MuleEvent} currently being processed.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void getMetadata(final String path, final MuleEvent muleEvent) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // final Map<String, Object> pathMetaData = getPathMetaData(hdfsPath);
    // // for (final Entry<String, Object> pathMetaDatum : pathMetaData.entrySet()) {
    // // muleEvent.setFlowVariable(pathMetaDatum.getKey(), pathMetaDatum.getValue());
    // // }
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Write the current payload to the designated path, either creating a new file or appending to an existing one.
    // *
    // * @param path
    // * the path of the file to write to.
    // * @param permission
    // * the file system permission to use if a new file is created, either in octal or symbolic format (umask).
    // * @param overwrite
    // * if a pre-existing file should be overwritten with the new content.
    // * @param bufferSize
    // * the buffer size to use when appending to the file.
    // * @param replication
    // * block replication for the file.
    // * @param blockSize
    // * the buffer size to use when appending to the file.
    // * @param ownerUserName
    // * the username owner of the file.
    // * @param ownerGroupName
    // * the group owner of the file.
    // * @param payload
    // * the payload to write to the file.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void write(
    // final String path, // NOSONAR
    // final String permission, final boolean overwrite, final int bufferSize, final int replication, final long blockSize, final String ownerUserName,
    // final String ownerGroupName, final InputStream payload) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath, getFileSystemPermission(permission), overwrite, bufferSize, (short) replication,
    // blockSize, null);
    // // IOUtils.copyLarge(payload, fsDataOutputStream);
    // // fsDataOutputStream.hsync();
    // // IOUtils.closeQuietly(fsDataOutputStream);
    //
    // if ((isNotBlank(ownerUserName)) || (isNotBlank(ownerGroupName))) {
    // fileSystem.setOwner(hdfsPath, ownerUserName, ownerGroupName);
    // }
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Append the current payload to a file located at the designated path. <b>Note:</b> by default the Hadoop server has the append option disabled. In order to be able append
    // any
    // * data to an existing file refer to dfs.support.append configuration parameter
    // *
    // * @param path
    // * the path of the file to write to.
    // * @param bufferSize
    // * the buffer size to use when appending to the file.
    // * @param payload
    // * the payload to append to the file.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void append(final String path, final int bufferSize, final InputStream payload) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // final FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsPath, bufferSize);
    // // IOUtils.copyLarge(payload, fsDataOutputStream);
    // // IOUtils.closeQuietly(fsDataOutputStream);
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Delete the file or directory located at the designated path.
    // *
    // * @param path
    // * the path of the file to delete.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void deleteFile(final String path) throws HDFSConnectorException {
    // deletePath(path, false);
    // }
    //
    // /**
    // * Delete the file or directory located at the designated path.
    // *
    // * @param path
    // * the path of the directory to delete.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void deleteDirectory(final String path) throws HDFSConnectorException {
    // deletePath(path, true);
    // }
    //
    // private void deletePath(final String path, final boolean recursive) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // fileSystem.delete(hdfsPath, recursive);
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Make the given file and all non-existent parents into directories. Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    // *
    // * @param path
    // * the path to create directories for.
    // * @param permission
    // * the file system permission to use when creating the directories, either in octal or symbolic format (umask).
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void makeDirectories(final String path, final String permission) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // FsPermission fsPermission = getFileSystemPermission(permission);
    // fileSystem.mkdirs(hdfsPath, fsPermission);
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Renames path target to path destination.
    // *
    // * @param source
    // * the source path to be renamed.
    // * @param target
    // * the target new path after rename.
    // * @return Boolean true if rename is successful.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public Boolean rename(final String source, final String target) throws HDFSConnectorException {
    // try {
    // return runHdfsPathAction(source, new HdfsPathAction<Boolean>() {
    //
    // public Boolean run(final Path hdfsPath) throws Exception { // NOSONAR
    // return fileSystem.rename(hdfsPath, new Path(target));
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * List the statuses of the files/directories in the given path if the path is a directory
    // *
    // * @param path
    // * the given path
    // * @param filter
    // * the user supplied path filter
    // * @return FileStatus the statuses of the files/directories in the given path
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public List<FileStatus> listStatus(final String path, final String filter) throws HDFSConnectorException {
    // try {
    // return runHdfsPathAction(path, new HdfsPathAction<List<FileStatus>>() {
    //
    // public List<FileStatus> run(final Path hdfsPath) throws Exception { // NOSONAR
    // if (StringUtils.isNotEmpty(filter)) {
    // final Pattern pattern = Pattern.compile(filter);
    // PathFilter pathFilter = new PathFilter() {
    //
    // @Override
    // public boolean accept(Path path) {
    // return isDirectory(path, pattern);
    // }
    // };
    // return Arrays.asList(fileSystem.listStatus(hdfsPath, pathFilter));
    // }
    // return Arrays.asList(fileSystem.listStatus(hdfsPath));
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // private boolean isDirectory(Path path, Pattern pattern) {
    // try {
    // if (fileSystem.isDirectory(path)) {
    // return true;
    // } else {
    // return pattern.matcher(path.toString()).matches();
    // }
    // } catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    // }
    //
    // /**
    // * Return all the files that match file pattern and are not checksum files. Results are sorted by their names.
    // *
    // * @param pathPattern
    // * a regular expression specifying the path pattern.
    // * @param filter
    // * the user supplied path filter
    // * @return FileStatus an array of paths that match the path pattern.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public List<FileStatus> globStatus(final String pathPattern, final PathFilter filter) throws HDFSConnectorException {
    // try {
    // return runHdfsPathAction(pathPattern, new HdfsPathAction<List<FileStatus>>() {
    //
    // public List<FileStatus> run(final Path hdfsPath) throws Exception { // NOSONAR
    // PathFilter nonNullPathFilter = (filter != null) ? filter : new PathFilter() {
    //
    // @Override
    // public boolean accept(Path path) {
    // return true;
    // }
    // };
    // FileStatus[] fileStatusesAsArray = fileSystem.globStatus(hdfsPath, nonNullPathFilter);
    // return (fileStatusesAsArray != null) ? Arrays.asList(fileStatusesAsArray) : new ArrayList<FileStatus>();
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Copy the source file on the local disk to the FileSystem at the given target path, set deleteSource if the source should be removed.
    // *
    // * @param deleteSource
    // * whether to delete the source.
    // * @param overwrite
    // * whether to overwrite a existing file.
    // * @param source
    // * the source path on the local disk.
    // * @param target
    // * the target path on the File System.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void copyFromLocalFile(final boolean deleteSource, final boolean overwrite, final String source, final String target) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(source, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // fileSystem.copyFromLocalFile(deleteSource, overwrite, hdfsPath, new Path(target));
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Copy the source file on the FileSystem to local disk at the given target path, set deleteSource if the source should be removed. useRawLocalFileSystem indicates whether to
    // * use RawLocalFileSystem as it is a non CRC File System.
    // *
    // * @param deleteSource
    // * whether to delete the source.
    // * @param useRawLocalFileSystem
    // * whether to use RawLocalFileSystem as local file system or not.
    // * @param source
    // * the source path on the File System.
    // * @param target
    // * the target path on the local disk.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void copyToLocalFile(final boolean deleteSource, final boolean useRawLocalFileSystem, final String source, final String target) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(source, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // fileSystem.copyToLocalFile(deleteSource, hdfsPath, new Path(target), useRawLocalFileSystem);
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // /**
    // * Set permission of a path (i.e., a file or a directory).
    // *
    // * @param path
    // * the path of the file or directory to set permission.
    // * @param permission
    // * the file system permission to be set.
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void setPermission(final String path, final String permission) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // fileSystem.setPermission(hdfsPath, getFileSystemPermission(permission));
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
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
    // * @throws HDFSConnectorException
    // * if any issue occurs during the execution.
    // */
    // public void setOwner(final String path, final String ownername, final String groupname) throws HDFSConnectorException {
    // try {
    // runHdfsPathAction(path, new VoidHdfsPathAction() {
    //
    // public void run(final Path hdfsPath) throws Exception { // NOSONAR
    // if ((isNotBlank(ownername)) || (isNotBlank(groupname))) {
    // fileSystem.setOwner(hdfsPath, ownername, groupname);
    // }
    // }
    // });
    // } catch (Exception e) {
    // throw new HDFSConnectorException(e);
    // }
    // }
    //
    // private void runHdfsPathAction(final String path, final VoidHdfsPathAction action) throws Exception { // NOSONAR
    // runHdfsPathAction(path, new HdfsPathAction<Void>() {
    //
    // public Void run(final Path hdfsPath) throws Exception { // NOSONAR
    // action.run(hdfsPath);
    // return null;
    // }
    // });
    // }
    //
    // private <T> T runHdfsPathAction(final String path, final HdfsPathAction<T> action) throws Exception { // NOSONAR
    // try {
    // final Path hdfsPath = new Path(path);
    // return action.run(hdfsPath);
    // } catch (final FileNotFoundException fnfe) {
    // // FileNotFound being an IOException: rethrow it wrapped with
    // // another exception to prevent the connection to be invalidated
    // throw new RuntimeException(fnfe);
    // }
    // }
    //
    // private FsPermission getFileSystemPermission(final String permission) {
    // return isBlank(permission) ? FsPermission.getDefault() : new FsPermission(permission);
    // }
    //
    // public AbstractConfig getConnection() {
    // return connection;
    // }
    //
    // public void setConnection(AbstractConfig connection) {
    // this.connection = connection;
    // this.setFileSystem(connection.getFileSystem());
    // }
    //
    // public void setFileSystem(final FileSystem fileSystem) {
    // this.fileSystem = fileSystem;
    // }
    //
    // private interface HdfsPathAction<T> {
    //
    // T run(Path hdfsPath) throws Exception; // NOSONAR
    // }
    //
    // private interface VoidHdfsPathAction {
    //
    // void run(Path hdfsPath) throws Exception; // NOSONAR
    // }
}
