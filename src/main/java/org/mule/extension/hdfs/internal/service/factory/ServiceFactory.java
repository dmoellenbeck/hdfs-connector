package org.mule.extension.hdfs.internal.service.factory;

import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.impl.FileSystemApiService;

public class ServiceFactory {

    public HdfsAPIService getService(HdfsConnection connection) {
        return new FileSystemApiService((FileSystemConnection)connection);
    }
}
