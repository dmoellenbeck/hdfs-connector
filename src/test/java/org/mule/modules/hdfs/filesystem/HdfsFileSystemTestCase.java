/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.FileNotFound;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.modules.hdfs.filesystem.exception.UnableToSeekToPosition;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
    public void consumeRelativePath() throws Exception {
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, 100, DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE)));
    }

    private void consumeAndValidateContent(URI uri, long startPosition, int bufferSize, String expectedContent) throws Exception {
        final List<DataChunk> contentSequence = new ArrayList<>();
        Consumer<DataChunk> fileContentConsumer = item -> contentSequence.add(item);
        hdfsFileSystem.consume(uri, startPosition, bufferSize, fileContentConsumer);
        long exptectedStartPosition = (startPosition > 0)? startPosition : 0;
        int expectedBufferSize = (bufferSize > 0)? bufferSize : 4096;
        validateConsumedContent(expectedContent, exptectedStartPosition, expectedBufferSize, contentSequence);
    }

    @Test
    public void consumeAbsolutePath() throws Exception {
        consumeAndValidateContent(URI.create(ABSOLUTE_FILE), 0, 100, DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(ABSOLUTE_FILE)));
    }

    @Test
    public void consumeFileSchemaPath() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(startsWith("Wrong FS"));
        hdfsFileSystem.consume(URI.create(FILE_SCHEMA_URI), 0, 100, item -> {});
    }

    @Test
    public void consumeURIWhenAuthorityDiffersFromUnderlayingFileSystem() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(startsWith("Wrong FS"));
        hdfsFileSystem.consume(URI.create("hdfs://fake:9000/myfile.txt"), 0, 100, item -> {});
    }

    @Test
    public void consumeHdfsSchemaPath() throws Exception {
        consumeAndValidateContent(URI.create(HDFS_SCHEMA_URI), 0, 100, DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(HDFS_SCHEMA_URI)));
    }

    @Test
    public void consumeRelativePathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.consume(URI.create(RELATIVE_FILE_THAT_DOES_NOT_EXIST), 0, 100, item -> {});
    }

    @Test
    public void consumeAbsolutePathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.consume(URI.create(ABSOLUTE_FILE_THAT_DOES_NOT_EXIST), 0, 100, item -> {});
    }

    @Test
    public void consumeHdfsSchemaPathThatDoesNotExist() throws Exception {
        expectedException.expect(FileNotFound.class);
        hdfsFileSystem.consume(URI.create(HDFS_SCHEMA_URI_THAT_DOES_NOT_EXIST), 0, 100, item -> {});
    }

    @Test
    public void consumeNullPath() throws Exception {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(equalTo("URI can not be null."));
        hdfsFileSystem.consume(null, 0, 100, item -> {});
    }

    private void validateConsumedContent(String expectedContent, long startPosition, int bufferSize, List<DataChunk> contentSequence) throws Exception {
        ByteArrayOutputStream contentCollector = new ByteArrayOutputStream();
        long currentPosition = startPosition;
        for (DataChunk item : contentSequence) {
            assertThat("Start byte is not following the sequence.", item.getStartByte(), equalTo(currentPosition));
            assertThat("Number of bytes read greater than buffer size.", item.getBytesRead(), lessThanOrEqualTo(bufferSize));
            contentCollector.write(item.getData());
            currentPosition = currentPosition + item.getBytesRead();
        }
        ByteArrayInputStream expectedDataStream = new ByteArrayInputStream(contentCollector.toByteArray());
        assertThat("Opened stream does not have the expected content.", IOUtils.toString(expectedDataStream), equalTo(expectedContent));
    }

    @Test
    public void thatConsumeRaisesConnectionExceptionWhenOpeningRelativeFileButClusterNotDeployed() throws Exception {
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", "")));
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystem.consume(URI.create(RELATIVE_FILE), 0, 100, item -> {});
    }

    @Test
    public void thatConsumeRaisesConnectionExceptionWhenOpeningAbsoluteFileButClusterNotDeployed() throws Exception {
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(createFileSystem(clusterAuthority(globalCluster).replaceAll(".$", "")));
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystem.consume(URI.create(ABSOLUTE_FILE), 0, 100, item -> {});
    }

    @Test
    public void thatConsumeReadsContentFromTheBeginningWhenStartPositionIsNegative() throws Exception {
        consumeAndValidateContent(URI.create(RELATIVE_FILE), -3, 100, DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE)));
    }

    @Test
    public void thatConsumeReadsContentFromTheBeginningWhenStartPositionIsZero() throws Exception {
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, 100, DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE)));
    }

    @Test
    public void thatConsumeThrowsExceptionWhenStartPositionOverflows() throws Exception {
        expectedException.expect(UnableToSeekToPosition.class);
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 205, 100, "");
    }

    @Test
    public void thatConsumeReadsContentFromStartPositionWhenStartPositionIsBetweenZeroAndLength() throws Exception {
        ByteArrayInputStream fileContentStream = new ByteArrayInputStream(DFSTestUtil.readFileBuffer(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE)));
        fileContentStream.skip(73);
        int bufferSize = fileContentStream.available();
        byte[] expectedContentAsBytes = new byte[bufferSize];
        IOUtils.read(fileContentStream, expectedContentAsBytes, 0, bufferSize);
        String expectedContent = new String(expectedContentAsBytes);
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 73, 100, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsNegative() throws Exception {
        String expectedContent = DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE));
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, -4, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsZero() throws Exception {
        String expectedContent = DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE));
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, 0, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsBetweenZeroAndLength() throws Exception {
        String expectedContent = DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE));
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, 33, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsGreaterThanLength() throws Exception {
        String expectedContent = DFSTestUtil.readFile(createFileSystem(clusterAuthority(globalCluster)), new Path(RELATIVE_FILE));
        consumeAndValidateContent(URI.create(RELATIVE_FILE), 0, 33, expectedContent);
    }

    @Test
    public void thatConsumeClosesStreamWhichItIsReadingFrom() throws Exception {
        FSDataInputStream mockedFsDataInputStream = Mockito.mock(FSDataInputStream.class);
        FileSystem mockedFileSystem = Mockito.mock(FileSystem.class);
        Mockito.when(mockedFileSystem.open(Mockito.any(Path.class), Mockito.anyInt())).thenReturn(mockedFsDataInputStream);
        HdfsFileSystem hdfsFileSystem = new HdfsFileSystem(mockedFileSystem);
        hdfsFileSystem.consume(URI.create(RELATIVE_FILE), 0, 100, item -> {});
        Mockito.verify(mockedFsDataInputStream, Mockito.times(1)).close();
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
