package org.mule.extension.hdfs.internal.connection;

import org.apache.hadoop.fs.FileSystem;

public class FileSystemConnection extends HdfsConnection {

    private FileSystem fileSystem;

    public FileSystemConnection(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
}
