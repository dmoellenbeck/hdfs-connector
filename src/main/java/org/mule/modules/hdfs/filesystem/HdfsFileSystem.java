/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.FileNotFound;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.util.Objects;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystem implements MuleFileSystem {

    private FileSystem fileSystem;

    public HdfsFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public InputStream open(URI uri) {
        try {
            Objects.requireNonNull(uri, "URI can not be null.");
            return fileSystem.open(new Path(uri));
        } catch (FileNotFoundException e) {
            throw new FileNotFound("File does not exist.", e);
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        }
    }

    @Override
    public FileSystemStatus fileSystemStatus() {
        try {
            FsStatus fsStatus = fileSystem.getStatus();
            return new FileSystemStatus(fsStatus.getCapacity(), fsStatus.getUsed(), fsStatus.getRemaining());
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        }
    }
}
