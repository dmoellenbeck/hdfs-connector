/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.source;

import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.runtime.api.execution.ExceptionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.function.Consumer;

/**
 * @author MuleSoft, Inc.
 */
public class SourceUtils {

    private static Logger logger = LoggerFactory.getLogger(SourceUtils.class);

    public static void executeOperation(HdfsConnection hdfsConnection, ExceptionCallback<?, Exception> exceptionCallback, Consumer<MuleFileSystem> block) {
        HdfsFileSystemProvider hdfsFileSystemProvider = new HdfsFileSystemProvider();
        MuleFileSystem fileSystem = hdfsFileSystemProvider.fileSystem(hdfsConnection);

        try {
            block.accept(fileSystem);
        } catch (Exception e) {
            logger.error("Unable to execute file system operation.");
            exceptionCallback.onException(new FileNotFoundException());
        }
    }

}
