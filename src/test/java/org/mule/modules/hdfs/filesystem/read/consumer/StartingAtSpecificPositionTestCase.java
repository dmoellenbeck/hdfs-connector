/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.read.consumer;

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
import org.mule.modules.hdfs.filesystem.exception.ConsumerAlreadyClosed;
import org.mule.modules.hdfs.filesystem.exception.ConsumerClosed;
import org.mule.modules.hdfs.filesystem.exception.UnableToSeekToPosition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class StartingAtSpecificPositionTestCase {

    private static final String FILE_PATH = "myFile.txt";
    private static final String MOCK_FILES = "mock_files/";
    @Mock
    private FSDataInputStream fsDataInputStream;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private FSDataInputStream fsDataInputStreamThatThrowsExceptionOnSecondCall;

    @Before
    public void setUp() throws Exception {
        mockFsDataInputStream();
        mockFsDataInputStreamThatThrowsExceptionOnSecondCall();
    }

    private void mockFsDataInputStreamThatThrowsExceptionOnSecondCall() throws Exception {
        Mockito.when(fsDataInputStreamThatThrowsExceptionOnSecondCall.read(Mockito.any(ByteBuffer.class)))
                .thenReturn(0)
                .thenThrow(new RuntimeException());
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
    public void thatConsumeReadsContentFromTheBeginningWhenStartPositionIsNegative() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(-3, 100, expectedContent);
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

    private void consumeAndValidateContent(long startPosition, int bufferSize, String expectedContent) throws Exception {
        final List<DataChunk> contentSequence = new ArrayList<>();
        Consumer<DataChunk> fileContentConsumer = item -> contentSequence.add(item);
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(startPosition, bufferSize, fsDataInputStream);
        consumer.consume(fileContentConsumer);
        long exptectedStartPosition = (startPosition > 0)? startPosition : 0;
        int expectedBufferSize = (bufferSize > 0)? bufferSize : 4096;
        validateConsumedContent(expectedContent, exptectedStartPosition, expectedBufferSize, contentSequence);
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
    public void thatConsumeReadsContentFromTheBeginningWhenStartPositionIsZero() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(0, 100, expectedContent);
    }

    @Test
    public void thatConsumeThrowsExceptionWhenStartPositionOverflows() throws Exception {
        expectedException.expect(UnableToSeekToPosition.class);
        consumeAndValidateContent(400, 100, "");
    }

    @Test
    public void thatConsumeReadsContentFromStartPositionWhenStartPositionIsBetweenZeroAndLength() throws Exception {
        InputStream fileContentStream = mockResourceStream(MOCK_FILES + FILE_PATH);
        fileContentStream.skip(73);
        int bufferSize = fileContentStream.available();
        byte[] expectedContentAsBytes = new byte[bufferSize];
        IOUtils.read(fileContentStream, expectedContentAsBytes, 0, bufferSize);
        String expectedContent = new String(expectedContentAsBytes);
        consumeAndValidateContent(73, 100, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsNegative() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(0, -4, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsZero() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(0, 0, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsBetweenZeroAndLength() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(0, 33, expectedContent);
    }

    @Test
    public void thatConsumeReadsContentWhenBlockSizeIsGreaterThanLength() throws Exception {
        String expectedContent = readMockFileAsString(MOCK_FILES + FILE_PATH);
        consumeAndValidateContent(0, 33, expectedContent);
    }

    @Test
    public void thatConsumeClosesStreamWhichItIsReadingFrom() throws Exception {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStream);
        consumer.consume(item -> {});
        Mockito.verify(fsDataInputStream, Mockito.times(1)).close();
    }

    @Test
    public void thatConsumeCanBeStopped() {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStream);
        List<DataChunk> dataChunks = new ArrayList<>();
        consumer.consume(item -> {
            dataChunks.add(item);
            consumer.close();
        });
        assertThat("Consumer expected to be stopped after reading one element", dataChunks, hasSize(1));
    }

    @Test
    public void thatConsumeClosesStreamWhichItIsReadingFromWhenItIsStopped() throws Exception {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStream);
        consumer.consume(item -> consumer.close());
        Mockito.verify(fsDataInputStream, Mockito.times(1)).close();
    }

    @Test
    public void thatYouCannotStopConsumptionTwice() throws Exception {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStreamThatThrowsExceptionOnSecondCall);
        consumer.close();
        expectedException.expect(ConsumerAlreadyClosed.class);
        consumer.close();
    }

    @Test
    public void thatStopConsumptionClosesStreamEvenIfNeverStartedToConsume() throws Exception {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStreamThatThrowsExceptionOnSecondCall);
        consumer.close();
        Mockito.verify(fsDataInputStreamThatThrowsExceptionOnSecondCall, Mockito.times(1))
                .close();
    }

    @Test
    public void thatDataCanNotBeConsumedAfterStopConsumption() throws Exception {
        StartingAtSpecificPosition consumer = new StartingAtSpecificPosition(0, 100, fsDataInputStreamThatThrowsExceptionOnSecondCall);
        consumer.close();
        expectedException.expect(ConsumerClosed.class);
        consumer.consume(item -> {});
    }
}
