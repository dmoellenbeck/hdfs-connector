/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.factory;

import static org.mule.extension.hdfs.internal.mapping.factory.MapperFactory.dozerMapper;

import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.HdfsAPIService;
import org.mule.extension.hdfs.internal.service.impl.FileSystemApiService;

public class ServiceFactory {

    public HdfsAPIService getService(HdfsConnection connection) {
        return new FileSystemApiService((FileSystemConnection) connection, dozerMapper());
    }
}
