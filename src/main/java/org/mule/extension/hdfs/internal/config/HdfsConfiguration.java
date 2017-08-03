/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.config;

import org.mule.extension.hdfs.internal.connection.provider.KerberosConnectionProvider;
import org.mule.extension.hdfs.internal.connection.provider.SimpleConnectionProvider;
import org.mule.extension.hdfs.internal.operation.HdfsOperations;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

@Operations({
        HdfsOperations.class
})
@ConnectionProviders({
        KerberosConnectionProvider.class,
        SimpleConnectionProvider.class
})
// @Sources(SubscribeChannelSource.class)
@Configuration(name = "hdfs")
@DisplayName("Hdfs")
public class HdfsConfiguration {

}
