/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.operation;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.mule.modules.hdfs.extension.dto.ReadParameters;
import org.mule.modules.hdfs.extension.operation.page.DataChunksPaginator;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class HdfsFileSystemOperationsTestCase {

    @InjectMocks
    private HdfsFileSystemOperations hdfsFileSystemOperations;
    @Mock
    private HdfsConnection hdfsConnection;
    @Mock
    private HdfsFileSystemProvider hdfsFileSystemProvider;
    @Mock
    private MuleFileSystem muleFileSystem;
    @Mock
    private DataChunksReader dataChunkReader;
    @Mock
    private DataChunk dataChunk;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        mockDataChunkReader();
        mockMuleFileSystem();
        mockHdfsFileSystemProvider();
    }

    private void mockDataChunkReader() {
        Mockito.when(dataChunkReader.hasNext())
                .thenReturn(true, false);
        Mockito.when(dataChunkReader.nextChunk())
                .thenReturn(dataChunk);
    }

    private void mockMuleFileSystem() {
        Mockito.when(muleFileSystem.openReader(Mockito.any(), Mockito.eq(20L), Mockito.eq(4096)))
                .thenReturn(dataChunkReader);
        Mockito.when(muleFileSystem.openReader(Mockito.any(), Mockito.eq(10L), Mockito.eq(1024)))
                .thenThrow(new ConnectionRefused("Could not contact the server.", null));
    }

    private void mockHdfsFileSystemProvider() {
        Mockito.when(hdfsFileSystemProvider.fileSystem(hdfsConnection))
                .thenReturn(muleFileSystem);
    }

    @Test
    public void thatReadIsReturningAValidPaginator() throws Exception {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setStartPosition(20L);
        readParameters.setPath("myFile.txt");
        readParameters.setChunkSize(4096);

        List<DataChunk> collectedDataChunks = collectDataChunksUsingPaginator(readParameters);
        assertThat(collectedDataChunks, hasItems(new DataChunk[] { dataChunk
        }));
    }

    private List<DataChunk> collectDataChunksUsingPaginator(ReadParameters readParameters) throws IOException {
        List<DataChunk> collectedDataChunks = new ArrayList<>();
        try (DataChunksPaginator dataChunksPaginator = hdfsFileSystemOperations.read(hdfsConnection, readParameters)) {
            List<DataChunk> dataChunkPage;
            do {
                dataChunkPage = dataChunksPaginator.getPage(hdfsConnection);
                collectedDataChunks.addAll(dataChunkPage);
            } while (!dataChunkPage.isEmpty());
        }
        return collectedDataChunks;
    }

    @Test
    public void thatReadIsThrowingMeaningfulExceptionForEmptyPath() {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setStartPosition(20L);
        readParameters.setPath(" ");
        readParameters.setChunkSize(4096);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("Invalid path. Path: %s is not a valid URI.", readParameters.getPath()));
        hdfsFileSystemOperations.read(hdfsConnection, readParameters);
    }

    @Test
    public void thatReadIsThrowingMeaningfulExceptionForPathThatHasOnlySchema() {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setStartPosition(20L);
        readParameters.setPath("http:");
        readParameters.setChunkSize(4096);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("Invalid path. Path: %s is not a valid URI.", readParameters.getPath()));
        hdfsFileSystemOperations.read(hdfsConnection, readParameters);
    }

    @Test
    public void thatReadIsThrowingMeaningfulExceptionForPathThatStartsInBackslash() {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setStartPosition(20L);
        readParameters.setPath("\\http");
        readParameters.setChunkSize(4096);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("Invalid path. Path: %s is not a valid URI.", readParameters.getPath()));
        hdfsFileSystemOperations.read(hdfsConnection, readParameters);
    }

    @Test
    public void thatReadIsPropagatingExceptionThrownByMuleFileSystem() {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setStartPosition(10L);
        readParameters.setPath("myFile.txt");
        readParameters.setChunkSize(1024);
        expectedException.expect(ConnectionRefused.class);
        hdfsFileSystemOperations.read(hdfsConnection, readParameters);
    }
}
