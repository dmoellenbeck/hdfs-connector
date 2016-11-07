/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension;

import org.mule.modules.hdfs.extension.connection.provider.Kerberos;
import org.mule.modules.hdfs.extension.connection.provider.Simple;
import org.mule.modules.hdfs.extension.enricher.IOExceptionException;
import org.mule.modules.hdfs.extension.operation.HDFSConnectorOperations;
import org.mule.modules.hdfs.extension.source.ReadSource;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.OnException;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.connector.ConnectionProviders;

/**
 * Hadoop Distributed File System (HDFS) Connector.
 * 
 */
@Extension(name = "HDFS", description = "HDFS Connector")
@OnException(IOExceptionException.class)
@ConnectionProviders({
        Simple.class,
        Kerberos.class
})
@Sources({ ReadSource.class
})
@Operations(HDFSConnectorOperations.class)
public class HDFSExtension {

}
