/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.source;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mule.modules.hdfs.extension.dto.ReadParameters;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.read.DataChunksConsumer;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReadSourceTestCase {

    @Mock
    private HdfsConnection validConnection;
    @Mock
    private HdfsConnection wrongConnection;
    @Mock
    private HdfsFileSystemProvider hdfsFileSystemProvider;
    @InjectMocks
    private ReadSource readSource;
    @Mock
    private MuleFileSystem validFileSystem;
    @Mock
    private MuleFileSystem fileSystemThrowingException;
    @Mock
    private SourceCallback<DataChunk, Attributes> sourceCallback;
    private List<DataChunk> dataChunksStartingFromZero;
    private List<DataChunk> dataChunksStartingFrom71;
    @Mock
    private ConnectionRefused fileSystemException;
    @Mock
    private DataChunksConsumer chunksConsumerFromZero;
    @Mock
    private DataChunksConsumer chunksConsumerFrom71;

    @Before
    public void setUp() {
        dataChunksStartingFromZero = mockDataChunksStartingFrom(0);
        dataChunksStartingFrom71 = mockDataChunksStartingFrom(71);
        mockValidFileSystem();
        mockFileSystemThrowingException();
        mockHdfsFileSystemProvider();
        mockChunksConsumerFromZero();
        mockChunksConsumerFrom71();
    }

    private void mockChunksConsumerFromZero() {
        Mockito.doAnswer(answerGeneratorFromDataChunks(dataChunksStartingFromZero))
                .when(chunksConsumerFromZero)
                .consume(Mockito.any());
    }

    private void mockChunksConsumerFrom71() {
        Mockito.doAnswer(answerGeneratorFromDataChunks(dataChunksStartingFrom71))
                .when(chunksConsumerFrom71)
                .consume(Mockito.any());
    }

    private void mockFileSystemThrowingException() {
        Mockito.doThrow(fileSystemException)
                .when(fileSystemThrowingException)
                .openConsumer(Mockito.any(), Mockito.anyLong(), Mockito.anyInt());
    }

    private void mockValidFileSystem() {
        Mockito.doReturn(chunksConsumerFromZero)
                .when(validFileSystem)
                .openConsumer(Mockito.any(), Mockito.eq(0L), Mockito.anyInt());
        Mockito.doReturn(chunksConsumerFrom71)
                .when(validFileSystem)
                .openConsumer(Mockito.any(), Mockito.eq(71L), Mockito.anyInt());
    }

    private Answer answerGeneratorFromDataChunks(List<DataChunk> dataChunks) {
        return new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Consumer<DataChunk> consumerToFeedMOckDataInto = (Consumer<DataChunk>) invocationOnMock.getArguments()[0];
                for (DataChunk dataChunk : dataChunks) {
                    consumerToFeedMOckDataInto.accept(dataChunk);
                }
                return null;
            }
        };
    }

    private List<DataChunk> mockDataChunksStartingFrom(int startPosition) {
        List<DataChunk> dataChunks = new ArrayList<>();
        long dynamicGeneratedStartPosition = startPosition;
        for (int i = 0; i < 3; i++) {
            dataChunks.add(new DataChunk(dynamicGeneratedStartPosition, 4096, new byte[4096]));
            dynamicGeneratedStartPosition += 4096;
        }
        dataChunks.add(new DataChunk(dynamicGeneratedStartPosition, -1, new byte[0]));
        return dataChunks;
    }

    private void mockHdfsFileSystemProvider() {
        Mockito.when(hdfsFileSystemProvider.fileSystem(validConnection))
                .thenReturn(validFileSystem);
        Mockito.when(hdfsFileSystemProvider.fileSystem(wrongConnection))
                .thenReturn(fileSystemThrowingException);
    }

    @Test
    public void thatSourceIsAbleToConsumeValidContentStartingFromBeginning() throws Exception {
        List<DataChunk> collectedDataChunks = collectDataChunksFromSource("hdfs://localhost:9000/myfile.txt", 0L, 4096, validConnection);
        assertThat(collectedDataChunks, hasItems(dataChunksStartingFromZero.toArray(new DataChunk[0])));
    }

    @Test
    public void thatSourceIsAbleToConsumeValidContentStartingFrom71() throws Exception {
        List<DataChunk> collectedDataChunks = collectDataChunksFromSource("hdfs://localhost:9000/myfile.txt", 71L, 4096, validConnection);
        assertThat(collectedDataChunks, hasItems(dataChunksStartingFrom71.toArray(new DataChunk[0])));
    }

    @Test
    public void thatSourceIsProperlyPropagatingException() throws Exception {
        List<Throwable> thrownExceptions = collectExceptionsFromSource("hdfs://localhost:9000/myfile.txt", 0L, 4096, wrongConnection);
        assertThat(thrownExceptions, hasSize(1));
        Throwable throwable = thrownExceptions.get(0);
        assertThat(throwable, sameInstance(fileSystemException));
    }

    @Test
    public void thatSourceClosesChunksConsumerAtOnStop() throws Exception {
        collectDataChunksFromSource("hdfs://localhost:9000/myfile.txt", 0L, 4096, validConnection);
        Mockito.verify(chunksConsumerFromZero, Mockito.times(1))
                .close();
    }

    private List<Throwable> collectExceptionsFromSource(String filePath, long startPosition, int blockSize, HdfsConnection hdfsConnection) throws Exception {
        List<Throwable> collectedExceptions = new ArrayList<>();
        final Lock consumptionEndedLock = new ReentrantLock();
        final Condition consumptionEnded = consumptionEndedLock.newCondition();
        collectThrownExceptionsOnSourceCallback(throwable -> {
            collectedExceptions.add(throwable);
            signalCondition(consumptionEndedLock, consumptionEnded);
        });
        populateReadSource(filePath, startPosition, blockSize, hdfsConnection);
        readSource.onStart(sourceCallback);
        waitCondition(consumptionEndedLock, consumptionEnded);
        readSource.onStop();
        return collectedExceptions;
    }

    private void collectThrownExceptionsOnSourceCallback(final Consumer<Throwable> exceptionConsumer) {
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Throwable throwable = (Throwable) invocationOnMock.getArguments()[0];
                exceptionConsumer.accept(throwable);
                return null;
            }
        })
                .when(sourceCallback)
                .onSourceException(Mockito.any());
    }

    private List<DataChunk> collectDataChunksFromSource(String filePath, long startPosition, int blockSize, HdfsConnection hdfsConnection)
            throws Exception {
        List<DataChunk> collectedDataChunks = new ArrayList<>();
        final Lock consumptionEndedLock = new ReentrantLock();
        final Condition consumptionEnded = consumptionEndedLock.newCondition();
        consumeDataSentToSourceCallback(dataChunk -> {
            if (dataChunk.getBytesRead() > 0) {
                collectedDataChunks.add(dataChunk);
            } else {
                collectedDataChunks.add(dataChunk);
                signalCondition(consumptionEndedLock, consumptionEnded);
            }
        });
        populateReadSource(filePath, startPosition, blockSize, hdfsConnection);
        readSource.onStart(sourceCallback);
        waitCondition(consumptionEndedLock, consumptionEnded);
        readSource.onStop();
        return collectedDataChunks;
    }

    private void consumeDataSentToSourceCallback(Consumer<DataChunk> consumer) {
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Result<DataChunk, Attributes> resultToCollect = (Result<DataChunk, Attributes>) invocationOnMock.getArguments()[0];
                DataChunk dataChunk = resultToCollect.getOutput();
                consumer.accept(dataChunk);
                return null;
            }
        })
                .when(sourceCallback)
                .handle(Mockito.any());
    }

    private void waitCondition(Lock consumptionEndedLock, Condition consumptionEnded) throws Exception {
        consumptionEndedLock.lock();
        try {
            consumptionEnded.await(10, TimeUnit.SECONDS);
        } finally {
            consumptionEndedLock.unlock();
        }
    }

    private void signalCondition(Lock consumptionEndedLock, Condition consumptionEnded) {
        consumptionEndedLock.lock();
        try {
            consumptionEnded.signalAll();
        } finally {
            consumptionEndedLock.unlock();
        }
    }

    private void populateReadSource(String path, Long startPosition, Integer blockSize, HdfsConnection hdfsConnection) {
        ReadParameters readParameters = new ReadParameters();
        readParameters.setPath(path);
        readParameters.setStartPosition(startPosition);
        readParameters.setChunkSize(blockSize);
        readSource.setReadParameters(readParameters);
        readSource.setPollingFrequency(10000L);
        readSource.setHdfsConnection(hdfsConnection);
    }

}
