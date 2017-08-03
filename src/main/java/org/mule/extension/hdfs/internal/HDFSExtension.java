package org.mule.extension.hdfs.internal;

import org.mule.extension.hdfs.api.error.HdfsErrorType;
import org.mule.extension.hdfs.internal.config.HdfsConfiguration;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
import org.mule.runtime.extension.api.annotation.error.ErrorTypes;

@Configurations({
        HdfsConfiguration.class
})
@Xml(prefix = "hdfs")
@Extension(name = "Hdfs", description = "Connector to manipulate specific data to Hadoop Distributed File System (HDFS).")
@ErrorTypes(HdfsErrorType.class)
public class HDFSExtension {

}
