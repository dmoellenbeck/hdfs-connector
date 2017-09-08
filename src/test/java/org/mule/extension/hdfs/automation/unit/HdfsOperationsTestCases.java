package org.mule.extension.hdfs.automation.unit;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.operation.HdfsOperations;
import org.mule.extension.hdfs.internal.service.impl.FileSystemApiService;

public class HdfsOperationsTestCases {

    HdfsOperations hdfsOperations = new HdfsOperations();
    @Mock
    FileSystemApiService fileSystemApiService;
    FileSystem fileSystem = Mockito.mock(FileSystem.class);
    HdfsConnection hdfsConnection = new FileSystemConnection(fileSystem);

    @Test
    public void testDeleteFile() {
        try {
            // fileSystem = spy(new LocalFileSystem());
            when(fileSystem.delete(any(Path.class), eq(false))).thenReturn(true);

            hdfsOperations.deleteFile(hdfsConnection, "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteDirectory() {
        try {

            // fileSystem = spy(new LocalFileSystem());
            when(fileSystem.delete(any(Path.class), eq(true))).thenReturn(true);
            // doCallRealMethod().when(fileSystem)
            // .delete(any(Path.class), eq(true));

            hdfsOperations.deleteDirectory(hdfsConnection, "zoo");

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMakeDirectories() {
        try {
            // fileSystem = spy(new LocalFileSystem());
            when(fileSystem.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(true);
            hdfsOperations.makeDirectories(hdfsConnection, "foo", "755");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testRename() {
        try {
            when(fileSystem.rename(any(Path.class), any(Path.class))).thenReturn(true);
            hdfsOperations.rename(hdfsConnection, "foo", "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testListStatus() {
        try {
            when(fileSystem.listStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
            hdfsOperations.listStatus(hdfsConnection, "foo", "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGlobStatus() {
        try {
            when(fileSystem.globStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
            hdfsOperations.globStatus(hdfsConnection, "foo", " ");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCopyFromLocalFile() {
        try {
            doNothing().when(fileSystem)
                    .copyFromLocalFile(anyBoolean(), anyBoolean(), any(Path.class), any(Path.class));
            hdfsOperations.copyFromLocalFile(hdfsConnection, false, false, "foo", "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCopyToLocalFile() {
        try {
            doNothing().when(fileSystem)
                    .copyToLocalFile(anyBoolean(), any(Path.class), any(Path.class), anyBoolean());
            hdfsOperations.copyToLocalFile(hdfsConnection, false, true, "foo", "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetPermission() {
        try {
            doNothing().when(fileSystem)
                    .setPermission(any(Path.class), any(FsPermission.class));
            hdfsOperations.setPermission(hdfsConnection, "foo", "755");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetOwner() {
        try {
            doNothing().when(fileSystem)
                    .setOwner(any(Path.class), anyString(), anyString());
            hdfsOperations.setOwner(hdfsConnection, "foo/oof", "foo", "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
