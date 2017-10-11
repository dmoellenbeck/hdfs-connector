/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
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
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.operation.HdfsOperations;
import org.mule.extension.hdfs.internal.service.exception.ExceptionMessages;
import org.mule.extension.hdfs.internal.service.impl.FileSystemApiService;
import org.mule.runtime.extension.api.exception.ModuleException;

import java.io.IOException;

public class HdfsOperationsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    HdfsOperations hdfsOperations = new HdfsOperations();
    @Mock
    FileSystemApiService fileSystemApiService;
    FileSystem fileSystem = Mockito.mock(FileSystem.class);
    HdfsConnection hdfsConnection = new FileSystemConnection(fileSystem);

    @Test
    public void testDeleteFile() {
        try {
            when(fileSystem.delete(any(Path.class), eq(false))).thenReturn(true);
            hdfsOperations.deleteFile(hdfsConnection, "zoo");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnsuccesfullyDeleteFile() throws IOException {
            expectedException.expect(ModuleException.class);
            expectedException.expectMessage(StringContains.containsString("Unable to delete file!"));
            when(fileSystem.delete(any(Path.class), eq(false))).thenReturn(false);
            hdfsOperations.deleteFile(hdfsConnection,"fdsdfs");

    }

    @Test
    public void testUnsuccesfullyDeleteFileWrongParams() throws IOException {
        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Invalid request data"));
        when(fileSystem.delete(any(Path.class), eq(false))).thenReturn(true);
        hdfsOperations.deleteFile(hdfsConnection,null);

    }

    @Test
    public void testDeleteDirectory() {
        try {

            when(fileSystem.delete(any(Path.class), eq(true))).thenReturn(true);
           
            hdfsOperations.deleteDirectory(hdfsConnection, "zoo");

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnsuccesfullyDeleteDirectory() throws IOException {
        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Unable to delete"));
        when(fileSystem.delete(any(Path.class), eq(false))).thenReturn(false);
        hdfsOperations.deleteDirectory(hdfsConnection,"fdsdfs");

    }

    @Test
    public void testMakeDirectories() {
        try {
            when(fileSystem.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(true);
            hdfsOperations.makeDirectories(hdfsConnection, "foo", "755");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void testUnsuccesfullyMakeDirectories() throws IOException {

            expectedException.expect(ModuleException.class);
            expectedException.expectMessage(StringContains.containsString("Unable to create directory!"));
            when(fileSystem.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(false);
            hdfsOperations.makeDirectories(hdfsConnection, "foo", "755");

    }

    @Test
    public void testUnsuccesfullyMakeDirectoriesWrongParams() throws IOException {

        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Invalid request data"));
        when(fileSystem.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(true);
        hdfsOperations.makeDirectories(hdfsConnection, null, "755");

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
    public void testUnsuccesfullyRename() throws IOException {

            expectedException.expect(ModuleException.class);
            expectedException.expectMessage(StringContains.containsString("Unable to rename path!"));

            when(fileSystem.rename(any(Path.class), any(Path.class))).thenReturn(false);
            hdfsOperations.rename(hdfsConnection, "foo", "zoo");

    }
    @Test
    public void testUnsuccesfullyRenameWrongParams() throws IOException {

        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Invalid request data"));

        when(fileSystem.rename(any(Path.class), any(Path.class))).thenReturn(true);
        hdfsOperations.rename(hdfsConnection, null, null);

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
    public void testListStatusWrongParams() throws IOException {

            expectedException.expect(ModuleException.class);
            expectedException.expectMessage(StringContains.containsString("Invalid request data"));
            when(fileSystem.listStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
            hdfsOperations.listStatus(hdfsConnection, null, null);

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
    public void testGlobStatusWrongParams() throws IOException {
            expectedException.expect(ModuleException.class);
            expectedException.expectMessage(StringContains.containsString("Invalid request data"));
            when(fileSystem.globStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
            hdfsOperations.globStatus(hdfsConnection, null, null);

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
    public void testCopyFromLocalFileWrongParams() throws IOException {
        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Invalid request data"));
        fileSystem.copyFromLocalFile(anyBoolean(), anyBoolean(), any(Path.class), any(Path.class));
        hdfsOperations.copyFromLocalFile(hdfsConnection, false, false, null, null);

    }

    @Test
    public void testUnsuccesfullyCopyFromLocalFile() {
        expectedException.expect(ModuleException.class);
        expectedException.expectMessage(StringContains.containsString("Invalid request data :Can not create a Path from a null string"));
        hdfsOperations.copyFromLocalFile(hdfsConnection, false, false, null, null);
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
    public void testUnsuccesfullyCopyToLocalFile() {
            expectedException.expectMessage(StringContains.containsString("Invalid request data :Can not create a Path from a null string"));
            hdfsOperations.copyToLocalFile(hdfsConnection, false, true, null, null);

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
    public void testUnsuccesfullySetPermission() {
        try {
            expectedException.expectMessage(StringContains.containsString("Invalid request data :Can not create a Path from a null string"));
            hdfsOperations.setPermission(hdfsConnection, null, "755");
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
