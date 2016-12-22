/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.read.reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReaderStartingAtSpecificPositionTestCase {

    private static final String FILE_PATH = "myFile.txt";
    private static final String MOCK_FILES = "mock_files/";
    @Mock
    private FSDataInputStream fsDataInputStream;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private FSDataInputStream fsDataInputStreamThatThrowsExceptionOnSecondCallToRead;
    @Mock
    private FSDataInputStream streamThrowingExceptionOnCallToAvailable;
    @Mock
    private FSDataInputStream streamThrowingExceptionOnCallToClose;
    @Mock
    private FSDataInputStream streamThrowingExceptionOnCallToSeek;
    @Mock
    private FSDataInputStream fsDataInputStreamWithNoDataAvailable;

    @Before
    public void setUp() throws Exception {
        mockFsDataInputStream();
        mockFsDataInputStreamThatThrowsExceptionOnSecondCall();
        mockStreamThrowingExceptionOnCallToAvailable();
        mockStreamThrowingExceptionOnCallToClose();
        mockStreamThrowingExceptionOnCallToSeek();
        mockStreamWithNoDataAvailable();
    }

    private void mockStreamWithNoDataAvailable() throws Exception {
        Mockito.when(streamThrowingExceptionOnCallToSeek.available())
                .thenReturn(0);
    }

    private void mockStreamThrowingExceptionOnCallToSeek() throws Exception {
        Mockito.doThrow(new IOException("Connection closed."))
                .when(streamThrowingExceptionOnCallToSeek)
                .seek(Mockito.anyLong());
    }

    private void mockStreamThrowingExceptionOnCallToClose() throws Exception {
        Mockito.doThrow(new IOException("Connection closed."))
                .when(streamThrowingExceptionOnCallToClose)
                .close();
    }

    private void mockStreamThrowingExceptionOnCallToAvailable() throws Exception {
        Mockito.doThrow(new IOException("Connection closed."))
                .when(streamThrowingExceptionOnCallToAvailable)
                .available();
    }

    private void mockFsDataInputStreamThatThrowsExceptionOnSecondCall() throws Exception {
        Mockito.when(fsDataInputStreamThatThrowsExceptionOnSecondCallToRead.available())
                .thenReturn(30);
        Mockito.when(fsDataInputStreamThatThrowsExceptionOnSecondCallToRead.read(Mockito.any(ByteBuffer.class)))
                .thenReturn(30)
                .thenThrow(new IOException());
    }

    private void mockFsDataInputStream() throws Exception {
        InputStream resourceStream = mockResourceStream(MOCK_FILES + FILE_PATH);
        ReadableByteChannel resourceChannel = Channels.newChannel(resourceStream);
        AtomicReference<Long> position = new AtomicReference<>(0l);
        Mockito.when(fsDataInputStream.available())
                .thenAnswer(new Answer<Integer>() {

                    @Override
                    public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return resourceStream.available();
                    }
                });
        Mockito.when(fsDataInputStream.getPos())
                .thenAnswer(new Answer<Long>() {

                    @Override
                    public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return position.get();
                    }
                });
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Long seekPosition = (Long) invocationOnMock.getArguments()[0];
                resourceStream.skip(seekPosition);
                position.set(seekPosition);
                return null;
            }
        })
                .when(fsDataInputStream)
                .seek(Mockito.anyLong());
        Mockito.doAnswer(new Answer<Integer>() {

            @Override
            public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
                int noOfBYtesRead = resourceChannel.read(buffer);
                Long currentPosition = position.get();
                position.set(currentPosition + noOfBYtesRead);
                return noOfBYtesRead;
            }
        })
                .when(fsDataInputStream)
                .read(Mockito.any(ByteBuffer.class));
    }

    @Test
    public void thatReaderReadsContentFromTheBeginningWhenStartPositionIsNegative() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(-3, 100, expectedContent);
    }

    private String readMockFileAsString(String mockFile) throws IOException {
        InputStream resourceStream = mockResourceStream(mockFile);
        return IOUtils.toString(resourceStream);
    }

    private InputStream mockResourceStream(String mockResource) {
        return this.getClass()
                .getClassLoader()
                .getResourceAsStream(mockResource);
    }

    private void readAndValidateContent(long startPosition, int bufferSize, String expectedContent) throws Exception {
        final List<DataChunk> contentSequence = new ArrayList<>();
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(startPosition, bufferSize, fsDataInputStream);
        while (reader.hasNext()) {
            contentSequence.add(reader.nextChunk());
        }
        long exptectedStartPosition = (startPosition > 0) ? startPosition : 0;
        int expectedBufferSize = (bufferSize > 0) ? bufferSize : 4096;
        validateConsumedContent(expectedContent, exptectedStartPosition, expectedBufferSize, contentSequence);
    }

    private void validateConsumedContent(String expectedContent, long startPosition, int bufferSize, List<DataChunk> contentSequence) throws Exception {
        ByteArrayOutputStream contentCollector = new ByteArrayOutputStream();
        long currentPosition = startPosition;
        for (DataChunk item : contentSequence) {
            assertThat("Start byte is not following the sequence.", item.getStartByte(), equalTo(currentPosition));
            assertThat("Number of bytes page greater than buffer size.", item.getBytesRead(), lessThanOrEqualTo(bufferSize));
            contentCollector.write(item.getData());
            currentPosition = currentPosition + item.getBytesRead();
        }
        ByteArrayInputStream expectedDataStream = new ByteArrayInputStream(contentCollector.toByteArray());
        assertThat("Opened stream does not have the expected content.", IOUtils.toString(expectedDataStream), equalTo(expectedContent));
    }

    @Test
    public void thatReaderReadsContentFromTheBeginningWhenStartPositionIsZero() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(0, 100, expectedContent);
    }

    @Test
    public void thatReaderThrowsExceptionWhenStartPositionOverflows() throws Exception {
        expectedException.expect(UnableToSeekToPosition.class);
        readAndValidateContent(400, 100, "");
    }

    @Test
    public void thatReaderReadsContentFromStartPositionWhenStartPositionIsBetweenZeroAndLength() throws Exception {
        InputStream fileContentStream = mockResourceStream(MOCK_FILES + FILE_PATH);
        fileContentStream.skip(73);
        int bufferSize = fileContentStream.available();
        byte[] expectedContentAsBytes = new byte[bufferSize];
        IOUtils.read(fileContentStream, expectedContentAsBytes, 0, bufferSize);
        String expectedContent = new String(expectedContentAsBytes);
        readAndValidateContent(73, 100, expectedContent);
    }

    @Test
    public void thatReaderReadsContentWhenBlockSizeIsNegative() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(0, -4, expectedContent);
    }

    @Test
    public void thatReaderReadsContentWhenBlockSizeIsZero() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(0, 0, expectedContent);
    }

    @Test
    public void thatReaderReadsContentWhenBlockSizeIsBetweenZeroAndLength() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(0, 33, expectedContent);
    }

    @Test
    public void thatReaderReadsContentWhenBlockSizeIsGreaterThanLength() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        readAndValidateContent(0, 2048, expectedContent);
    }

    @Test
    public void thatNextChunkIsThrowingApppropriateExceptionWhenConnectionIsLostBeforeCallToRead() {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStreamThatThrowsExceptionOnSecondCallToRead);
        reader.nextChunk();
        expectedException.expect(ConnectionLost.class);
        reader.nextChunk();
    }

    @Test
    public void thatNextChunkIsThrowingApppropriateExceptionWhenCalledButNoMoreDataAvailable() {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStreamWithNoDataAvailable);
        expectedException.expect(NoMoreDataAvailable.class);
        reader.nextChunk();
    }

    @Test
    public void thatCloseThrowsExceptionWhenCalledTwice() throws Exception {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStream);
        reader.close();
        expectedException.expect(ReaderAlreadyClosed.class);
        reader.close();
    }

    @Test
    public void thatCloseClosesStreamEvenWhenCalledBeforeConsumingAnything() throws Exception {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStream);
        reader.close();
        Mockito.verify(fsDataInputStream, Mockito.times(1))
                .close();
    }

    @Test
    public void thatNextChunkThrowsExceptionWhenCalledAfterClosingReader() throws Exception {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStream);
        reader.close();
        expectedException.expect(ReaderClosed.class);
        reader.nextChunk();
    }

    @Test
    public void thatHasNextThrowsExceptionIfCalledAfterClosingReader() throws Exception {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, fsDataInputStream);
        reader.close();
        expectedException.expect(ReaderClosed.class);
        reader.hasNext();
    }

    @Test
    public void thatHasNextThrowsAppropriateExceptionWhenConnectionIsLost() throws Exception {
        FSDataInputStream streamThrowingExceptionOnCallOfAnyMethod = Mockito.mock(FSDataInputStream.class);
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, streamThrowingExceptionOnCallOfAnyMethod);
        // available is called in constructor so we only mock it to throw exception now
        Mockito.doThrow(new IOException("Connection closed."))
                .when(streamThrowingExceptionOnCallOfAnyMethod)
                .available();
        expectedException.expect(ConnectionLost.class);
        reader.hasNext();
    }

    @Test
    public void thatAppropriateExceptionIsThrownWhenConnectionIsLostBeforeSeekingToStartPosition() throws Exception {
        expectedException.expect(ConnectionLost.class);
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, streamThrowingExceptionOnCallToAvailable);

        expectedException.expect(ConnectionLost.class);
        reader = new ReaderStartingAtSpecificPosition(0, 100, streamThrowingExceptionOnCallToSeek);
    }

    @Test
    public void thatAppropriateExceptionIsThrownWhenConnectionIsLostBeforeClosingReader() throws Exception {
        ReaderStartingAtSpecificPosition reader = new ReaderStartingAtSpecificPosition(0, 100, streamThrowingExceptionOnCallToClose);
        expectedException.expect(ConnectionLost.class);
        reader.close();
    }
}
