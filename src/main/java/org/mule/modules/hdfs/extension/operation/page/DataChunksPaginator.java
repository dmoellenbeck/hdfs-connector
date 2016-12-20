/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.operation.page;

import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;
import org.mule.runtime.extension.api.runtime.streaming.PagingProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author MuleSoft, Inc.
 */
public class DataChunksPaginator implements PagingProvider<HdfsConnection, DataChunk> {

    private DataChunksReader dataChunksReader;

    public DataChunksPaginator(DataChunksReader dataChunksReader) {
        Objects.requireNonNull(dataChunksReader, "Given reader can not be null");
        this.dataChunksReader = dataChunksReader;
    }

    @Override
    public List<DataChunk> getPage(HdfsConnection hdfsConnection) {
        List<DataChunk> pageOfDataChunks = new ArrayList<>();
        if (dataChunksReader.hasNext()) {
            pageOfDataChunks.add(dataChunksReader.nextChunk());
        }
        return pageOfDataChunks;
    }

    @Override
    public Optional<Integer> getTotalResults(HdfsConnection hdfsConnection) {
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        dataChunksReader.close();
    }
}
