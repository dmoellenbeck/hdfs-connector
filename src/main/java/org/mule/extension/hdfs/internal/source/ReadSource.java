package org.mule.extension.hdfs.internal.source;

import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.api.MetaData;
import org.mule.extension.hdfs.internal.service.factory.ServiceFactory;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.Streaming;
import org.mule.runtime.extension.api.annotation.metadata.MetadataScope;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.source.EmitsResponse;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.extension.api.metadata.NullQueryMetadataResolver;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;

import java.io.IOException;
import java.io.Serializable;

import static org.mule.extension.hdfs.internal.error.HdfsErrorType.CONNECTIVITY;

@Alias("read")
@EmitsResponse
@Streaming
@MediaType(MediaType.ANY)
@MetadataScope(outputResolver = NullQueryMetadataResolver.class)
public class ReadSource extends Source<Object, Serializable> {

    private ServiceFactory serviceFactory = new ServiceFactory();
    /**
     * Read the content of a file designated by its path
     *
     * @param path
     * the path of the file to read.
     * @param bufferSize
     * the buffer size to use when reading the file.
     * @return the result from executing the rest of the flow.
     */
    @Parameter
    private String path;
    @Parameter
    @Optional(defaultValue = "4096")
    private int bufferSize;

    @Connection
    private ConnectionProvider<HdfsConnection> connectionProvider;

    @Override
    public void onStart(SourceCallback<Object, java.io.Serializable> sourceCallback) throws MuleException {
        try {
            doStart(sourceCallback);
        } catch (Exception e) {
            sourceCallback.onConnectionException(ConnectionException.class.cast(e));
        }
    }

    @Override
    public void onStop() {
        // Do nothing.
    }

    private void doStart(SourceCallback sourceCallback) throws IOException {
        try {
            HdfsAPIService hdfsApiService = serviceFactory.getService(connectionProvider.connect());
            MetaData metaData = hdfsApiService.getMetadata(path);
            sourceCallback.handle(Result.builder().attributes(metaData)
                    .output(hdfsApiService.read(path, bufferSize))
                    .build());
        } catch (ConnectionException e) {
            throw new ModuleException(e.getMessage(), CONNECTIVITY, e);
        }
    }
}
