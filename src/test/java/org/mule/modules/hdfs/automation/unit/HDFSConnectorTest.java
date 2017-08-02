///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.unit;
//
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.permission.FsPermission;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.runners.MockitoJUnitRunner;
//import org.mule.api.callback.SourceCallback;
//import org.mule.modules.hdfs.HDFSConnector;
//import org.mule.modules.hdfs.utils.MyMuleEvent;
//import org.mule.modules.tests.ConnectorTestUtils;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.junit.Assert.*;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyBoolean;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Mockito.anyInt;
//import static org.mockito.Mockito.*;
//
//@RunWith(MockitoJUnitRunner.class)
//public class HDFSConnectorTest {
//
//    @Mock
//    private FileSystem fileSystem;
//
//    @Mock
//    private SourceCallback readSourceCallback;
//
//    private HDFSConnector connector;
//
//    @Before
//    public void setUp() throws Exception {
//        this.connector = new HDFSConnector();
//        this.connector.setFileSystem(fileSystem);
//    }
//
//    @Test
//    public void testRead() {
//        try {
//            FSDataInputStream inputStream = Mockito.mock(FSDataInputStream.class);
//            when(fileSystem.open(any(Path.class), anyInt())).thenReturn(inputStream);
//
//            connector.read("foo", 4084, readSourceCallback);
//            Mockito.verify(readSourceCallback, times(1))
//                    .process(Mockito.any(), Mockito.anyMap());
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testGetMetadata() {
//        try {
//            Map<String, Object> metadata = mockGetPathMetaData(true);
//            MyMuleEvent muleEvent = new MyMuleEvent();
//            connector.getMetadata("foo", muleEvent);
//            assertEquals(muleEvent.getFlowVariable(connector.HDFS_PATH_EXISTS), metadata.get(connector.HDFS_PATH_EXISTS));
//            assertEquals(muleEvent.getFlowVariable(connector.HDFS_CONTENT_SUMMARY), metadata.get(connector.HDFS_CONTENT_SUMMARY));
//            assertEquals(muleEvent.getFlowVariable(connector.HDFS_FILE_CHECKSUM), metadata.get(connector.HDFS_FILE_CHECKSUM));
//            assertEquals(muleEvent.getFlowVariable(connector.HDFS_FILE_STATUS), metadata.get(connector.HDFS_FILE_STATUS));
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testDeleteFile() {
//        try {
//            fileSystem = spy(new LocalFileSystem());
//            doCallRealMethod().when(fileSystem)
//                    .delete(any(Path.class), eq(false));
//            connector.deleteFile("zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testDeleteDirectory() {
//        try {
//            fileSystem = spy(new LocalFileSystem());
//            doCallRealMethod().when(fileSystem)
//                    .delete(any(Path.class), eq(true));
//            connector.deleteDirectory("zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testMakeDirectories() {
//        try {
//            fileSystem = spy(new LocalFileSystem());
//            doCallRealMethod().when(fileSystem)
//                    .mkdirs(any(Path.class), any(FsPermission.class));
//            connector.makeDirectories("foo", "755");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testRename() {
//        try {
//            when(fileSystem.rename(any(Path.class), any(Path.class))).thenReturn(true);
//            assertTrue(connector.rename("foo", "zoo"));
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testListStatus() {
//        try {
//            when(fileSystem.listStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
//            connector.listStatus("foo", "zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testGlobStatus() {
//        try {
//            when(fileSystem.globStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[0]);
//            connector.globStatus("foo", new PathFilter() {
//
//                @Override
//                public boolean accept(Path path) {
//                    return false;
//                }
//            });
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testCopyFromLocalFile() {
//        try {
//            doNothing().when(fileSystem)
//                    .copyFromLocalFile(anyBoolean(), anyBoolean(), any(Path.class), any(Path.class));
//            connector.copyFromLocalFile(false, false, "foo", "zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testCopyToLocalFile() {
//        try {
//            doNothing().when(fileSystem)
//                    .copyToLocalFile(anyBoolean(), any(Path.class), any(Path.class), anyBoolean());
//            connector.copyToLocalFile(false, true, "foo", "zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testSetPermission() {
//        try {
//            doNothing().when(fileSystem)
//                    .setPermission(any(Path.class), any(FsPermission.class));
//            connector.setPermission("foo", "755");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    @Test
//    public void testSetOwner() {
//        try {
//            doNothing().when(fileSystem)
//                    .setOwner(any(Path.class), anyString(), anyString());
//            connector.setOwner("foo/oof", "foo", "zoo");
//        } catch (Exception e) {
//            fail(ConnectorTestUtils.getStackTrace(e));
//        }
//    }
//
//    private Map<String, Object> mockGetPathMetaData(boolean state) throws IOException {
//        Map<String, Object> metaData = new HashMap<String, Object>();
//        metaData.put(connector.HDFS_PATH_EXISTS, state);
//        when(fileSystem.exists(any(Path.class))).thenReturn(state);
//        ContentSummary summary = Mockito.mock(ContentSummary.class);
//        metaData.put(connector.HDFS_CONTENT_SUMMARY, summary);
//        when(fileSystem.getContentSummary(any(Path.class))).thenReturn(summary);
//        FileStatus status = Mockito.mock(FileStatus.class);
//        metaData.put(connector.HDFS_FILE_STATUS, status);
//        when(fileSystem.getFileStatus(any(Path.class))).thenReturn(status);
//        when(fileSystem.isDirectory(any(Path.class))).thenReturn(false);
//        FileChecksum checksum = Mockito.mock(FileChecksum.class);
//        metaData.put(connector.HDFS_FILE_CHECKSUM, checksum);
//        when(fileSystem.getFileChecksum(any(Path.class))).thenReturn(checksum);
//        return metaData;
//    }
//}
