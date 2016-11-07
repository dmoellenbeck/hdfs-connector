/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.FileNotFound;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;

import java.io.InputStream;
import java.net.URI;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystemTestCase extends HdfsClusterDependentTestBase {

    private static final String RELATIVE_FILE = "myFile.txt";
    private static final String RELATIVE_FILE_THAT_DOES_NOT_EXIST = "thisDoesNotExist.txt";
    private static final String ABSOLUTE_FILE = "/myFile.txt";
    private static final String ABSOLUTE_FILE_THAT_DOES_NOT_EXIST = "/thisDoesNotExist.txt";
    private static final String GLOBAL_CLUSTER_TEMP_DIR = "globalClusterTempDir";
    private static String FILE_SCHEMA_URI;
    private static String HDFS_SCHEMA_URI;
    private static String HDFS_SCHEMA_URI_THAT_DOES_NOT_EXIST;
    private HdfsFileSystem hdfsFileSystem;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @ClassRule
    public static TemporaryFolder globalClusterTempFolder = new TemporaryFolder();
    private static MiniDFSCluster globalCluster;
    @Rule
    public TemporaryFolder localClusterTempFolder = new TemporaryFolder();

    @BeforeClass
    public static void startGlobalCluster() throws Exception {
        globalCluster = startCluster(globalClusterTempFolder, GLOBAL_CLUSTER_TEMP_DIR);
    }

    @AfterClass
    public static void stopGlobalCluster() throws Exception {
        stopCluster(globalCluster);
    }

    @Before
    public void setUp() throws Exception {
        createFileIntoCluster(globalCluster, RELATIVE_FILE);
        createFileIntoCluster(globalCluster, ABSOLUTE_FILE);
        FILE_SCHEMA_URI = "file://" + clusterAuthority(globalCluster) + ABSOLUTE_FILE;
        HDFS_SCHEMA_URI = "hdfs://" + clusterAuthority(globalCluster) + ABSOLUTE_FILE;
        HDFS_SCHEMA_URI_THAT_DOES_NOT_EXIST = "hdfs://" + clusterAuthority(globalCluster) + ABSOLUTE_FILE_THAT_DOES_NOT_EXIST;
        hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster)));
    }

    @Test
    public void openRelativePath() throws Exception {
        openAndValidateContent(URI.create(RELATIVE_FILE), DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE)));
    }

    private void openAndValidateContent(URI uri, String expectedContent) throws Exception {
        InputStream openedStream = hdfsFileSystem.open(uri);
        validateOpenedStreamContent(expectedContent, openedStream);
    }

    @Test
    public void openAbsolutePath() throws Exception {
        openAndValidateContent(URI.create(ABSOLUTE_FILE), DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(ABSOLUTE_FILE)));
    }

    @Test
    public void openFileSchemaPath() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(startsWith("Wrong FS"));
        hdfsFileSystem.open(URI.create(FILE_SCHEMA_URI));
    }

    @Test
    public void openURIWhenAuthorityDiffersFromUnderlayingFileSystem() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(startsWith("Wrong FS"));
        hdfsFileSystem.open(URI.create("hdfs://fake:9000/myfile.txt"));
    }

    @Test
    public void openHdfsSchemaPath() throws Exception {
        openAndValidateContent(URI.create(HDFS_SCHEMA_URI), DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(HDFS_SCHEMA_URI)));
    }

    @Test
    public void openRelativePathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.open(URI.create(RELATIVE_FILE_THAT_DOES_NOT_EXIST));
    }

    @Test
    public void openAbsolutePathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.open(URI.create(ABSOLUTE_FILE_THAT_DOES_NOT_EXIST));
    }

    @Test
    public void openHdfsSchemaPathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.open(URI.create(HDFS_SCHEMA_URI_THAT_DOES_NOT_EXIST));
    }

    @Test
    public void openNullPath() throws Exception {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(equalTo("URI can not be null."));
        hdfsFileSystem.open(null);
    }

    private void validateOpenedStreamContent(String expectedContent, InputStream openedStream) throws Exception {
        assertThat("Opened stream can not be null.", openedStream, notNullValue());
        assertThat("Opened stream does not have the expected content.", IOUtils.toString(openedStream), equalTo(expectedContent));
    }

    @Test
    public void thatConnectionExceptionIsRaisedWhenOpeningRelativeFileButClusterNotDeployed() throws Exception {
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", "")));
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystem.open(URI.create(RELATIVE_FILE));
    }

    @Test
    public void thatConnectionExceptionIsRaisedWhenOpeningAbsoluteFileButClusterNotDeployed() throws Exception {
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", "")));
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystem.open(URI.create(ABSOLUTE_FILE));
    }

    @Test
    public void thatGetStatusMethodIsReturningDataInGoodConditions() {
        FileSystemStatus fileSystemStatus = hdfsFileSystem.fileSystemStatus();
        assertThat(fileSystemStatus, notNullValue());
    }

    @Test
    public void thatGetStatusIsThrowingConnectionRefusedWhenServerCanNotBeReached() throws Exception {
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", "")));
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystem.fileSystemStatus();
    }

    @Test
    public void thatGetStatusIsThrowingRuntimeIOWhenUnexpectedIOExceptionIsThrown() throws Exception {
        FileSystem fileSystem = createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", ""));
        fileSystem.close();
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(fileSystem);
        expectedException.expect(RuntimeIO.class);
        hdfsFileSystem.fileSystemStatus();
    }

}
