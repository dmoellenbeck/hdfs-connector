/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * @author MuleSoft, Inc.
 */
public abstract class HdfsClusterDependentTestBase {

    protected static void stopCluster(MiniDFSCluster hdfsCluster) throws Exception {
        hdfsCluster.shutdown();
    }

    protected static MiniDFSCluster startCluster(TemporaryFolder tempFolder, String clusterTempDir) throws Exception {
        File clusterBaseDir = tempFolder.newFolder(clusterTempDir)
                .getAbsoluteFile();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterBaseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        return builder.build();
    }

    protected static FileSystem createFileSystem(String authority) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + authority);
        return FileSystem.get(conf);
    }

    protected void createFileIntoCluster(MiniDFSCluster hdfsCluster, String validFile) throws Exception {
        DFSTestUtil.createFile(createFileSystem(clusterAuthority(hdfsCluster)), new Path(validFile), 162, (short) 1, 100);
    }

    protected String clusterAuthority(MiniDFSCluster hdfsCluster) {
        return hdfsCluster.getURI()
                .getAuthority();
    }
}
