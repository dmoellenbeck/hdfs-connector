/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal;

import org.mule.extension.hdfs.internal.error.HdfsErrorType;
import org.mule.extension.hdfs.internal.config.HdfsConfiguration;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
import org.mule.runtime.extension.api.annotation.error.ErrorTypes;

@Configurations({
        HdfsConfiguration.class
})
@Xml(prefix = "hdfs")
@Extension(name = "Hdfs")
@ErrorTypes(HdfsErrorType.class)
public class HDFSExtension {

}
