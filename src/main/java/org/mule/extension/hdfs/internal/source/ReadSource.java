package org.mule.extension.hdfs.internal.source;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;

import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.factory.ServiceFactory;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.Streaming;
import org.mule.runtime.extension.api.annotation.metadata.MetadataScope;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.source.EmitsResponse;
import org.mule.runtime.extension.api.metadata.NullQueryMetadataResolver;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.weave.v2.grammar.structure.Object;
import scala.Serializable;

@Alias("read")
@EmitsResponse
@Streaming
@MetadataScope(outputResolver = NullQueryMetadataResolver.class)
public class ReadSource extends Source<Object, Serializable> {

    private ServiceFactory serviceFactory = new ServiceFactory();

    @Parameter
    private String path;
    @Parameter
    @Optional(defaultValue = "4096")
    private int bufferSize;

    @Connection
    private HdfsConnection connection;

    @Override
    public void onStart(SourceCallback<Object, Attributes> sourceCallback) throws MuleException {

        try {
            doStart(sourceCallback);
        } catch (Exception e) {
            sourceCallback.onSourceException(e);
        }
    }

    private void doStart(SourceCallback sourceCallback) throws IOException {

        HdfsAPIService hdfsApiService = serviceFactory.getService(connection);

        Result.Builder<Object, Serializable> resultBuilder = Result.builder();

        HashMap<String, Object> metaData = (HashMap<String, Object>) hdfsApiService.getMetadata(path);
        InputStream is = hdfsApiService.read(path, bufferSize);

        Result<Object, Serializable> result = resultBuilder.attributes(metaData)
                .output(is)
                .build();

        sourceCallback.handle(result);

    }

    @Override
    public void onStop() {

    }

}
