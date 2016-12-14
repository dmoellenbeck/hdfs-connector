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
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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

    @Before
    public void setUp() {
        mockValidFileSystem();
        mockHdfsFileSystemProvider();
    }

    private void mockValidFileSystem() {
        List<DataChunk> dataChunksStartingFromZero = dataChunksStartingFrom(0);
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Consumer<DataChunk> consumerToFeedMOckDataInto = (Consumer<DataChunk>) invocationOnMock.getArguments()[3];
                for (DataChunk dataChunk : dataChunksStartingFromZero) {
                    consumerToFeedMOckDataInto.accept(dataChunk);
                }
                return null;
            }
        })
                .when(validFileSystem)
                .consume(Mockito.any(), Mockito.anyLong(), Mockito.anyInt(), Mockito.any());
    }

    private List<DataChunk> dataChunksStartingFrom(int startPosition) {
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
    }

    @Test
    public void thatSourceIsAbleToConsumeValidContentStartingFromBeginning() throws Exception {
        SourceCallback<DataChunk, Attributes> sourceCallback = Mockito.mock(SourceCallback.class);
        List<DataChunk> collectedDataChunks = new ArrayList<>();
        Lock consumptionEnded = new ReentrantLock();
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Result<DataChunk, Attributes> resultToCollect = (Result<DataChunk, Attributes>) invocationOnMock.getArguments()[0];
                DataChunk dataChunk = resultToCollect.getOutput();
                if (dataChunk.getBytesRead() > 0) {
                    collectedDataChunks.add(dataChunk);
                } else {
                    collectedDataChunks.add(dataChunk);
                    readSource.onStop();

                    consumptionEnded.lock();
                    try {
                        consumptionEnded.newCondition()
                                .signalAll();
                    } finally {
                        consumptionEnded.unlock();
                    }
                }

                return null;
            }
        })
                .when(sourceCallback)
                .handle(Mockito.any());
        populateReadSource("hdfs://localhost:9000/myfile.txt", 0L, 4096, validConnection);
        readSource.onStart(sourceCallback);

        consumptionEnded.lock();
        try {
            consumptionEnded.newCondition()
                    .await();
        } finally {
            consumptionEnded.unlock();
        }

        System.out.println("consumedData: " + collectedDataChunks);
    }

    private void populateReadSource(String path, Long startPosition, Integer blockSize, HdfsConnection hdfsConnection) {
        readSource.setPath(path);
        readSource.setStartPosition(startPosition);
        readSource.setBlockSize(blockSize);
        readSource.setPollingFrequency(10000L);
        readSource.setHdfsConnection(hdfsConnection);
    }

}
