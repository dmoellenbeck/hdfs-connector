/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.operation.page;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.ConnectionLost;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class DataChunksPaginatorTestCase {

    @Mock
    private DataChunksReader dataChunksReader;
    @Mock
    private DataChunksReader dataChunksReaderThatThrowsExceptionOnHasNext;
    @Mock
    private DataChunksReader dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk;
    @Mock
    private HdfsConnection hdfsConnection;
    @Mock
    private DataChunk dataChunk1;
    @Mock
    private DataChunk dataChunk2;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        mockDataChunksReader();
        mockDataChunksReaderThatThrowsExceptionOnHasNext();
        mockDataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk();
    }

    private void mockDataChunksReaderThatThrowsExceptionOnHasNext() {
        Mockito.when(dataChunksReaderThatThrowsExceptionOnHasNext.hasNext())
                .thenThrow(new ConnectionLost("Connection closed.", null));
    }

    private void mockDataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk() {
        Mockito.when(dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk.hasNext())
                .thenReturn(true);
        Mockito.when(dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk.nextChunk())
                .thenThrow(new ConnectionLost("Connection closed.", null));
        Mockito.doThrow(new ConnectionLost("Connection closed.", null))
                .when(dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk)
                .close();
    }

    private void mockDataChunksReader() {
        Mockito.when(dataChunksReader.hasNext())
                .thenReturn(true, true, false);
        Mockito.when(dataChunksReader.nextChunk())
                .thenReturn(dataChunk1, dataChunk2);
    }

    @Test
    public void thatPaginatorDoesNotAcceptNullReader() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Given reader can not be null");
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(null);
    }

    @Test
    public void thatGetTotalResultsAlwaysReturnsNullOptional() {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReader);
        int randNoOfRepeat = Math.abs((int) Math.random() % 100) + 1;
        for (int i = 0; i < randNoOfRepeat; i++) {
            assertThat(dataChunksPaginator.getTotalResults(hdfsConnection), is(Optional.empty()));
        }
    }

    @Test
    public void thatPaginatorIsReturningOneItemAtTheTimeInPageUntilFinishes() {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReader);
        collectPagesAndValidate(dataChunksPaginator, new DataChunk[] {
                dataChunk1,
                dataChunk2
        });
    }

    @Test
    public void thatPaginatorIsPropagatingExceptionThrownByHasNextFromReader() {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReaderThatThrowsExceptionOnHasNext);
        expectedException.expect(ConnectionLost.class);
        dataChunksPaginator.getPage(hdfsConnection);
    }

    @Test
    public void thatPaginatorIsPropagatingExceptionThrownByNextChunkFromReader() {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk);
        expectedException.expect(ConnectionLost.class);
        dataChunksPaginator.getPage(hdfsConnection);
    }

    @Test
    public void thatCloseIsClosingReader() throws Exception {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReader);
        dataChunksPaginator.close();
        Mockito.verify(dataChunksReader, Mockito.times(1))
                .close();
    }

    @Test
    public void thatCloseIsPropagatingExceptionThrownByReader() throws Exception {
        DataChunksPaginator dataChunksPaginator = new DataChunksPaginator(dataChunksReaderThatThrowsExceptionsOnCloseAndNextChunk);
        expectedException.expect(ConnectionLost.class);
        dataChunksPaginator.close();
    }

    private void collectPagesAndValidate(DataChunksPaginator dataChunksPaginator, DataChunk[] expectedDataChunks) {
        List<DataChunk> collectedDataChunks = new ArrayList<>();
        List<DataChunk> page;
        do {
            page = dataChunksPaginator.getPage(hdfsConnection);
            collectedDataChunks.addAll(page);
        } while (!page.isEmpty());
        assertThat(collectedDataChunks, hasItems(expectedDataChunks));
    }

}
