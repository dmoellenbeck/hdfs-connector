/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;

import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystemProviderTestCase extends HdfsClusterDependentTestBase {

    public static final String CLUSTER_AUTHORITY = "localhost:9000";
    private HdfsFileSystemProvider hdfsFileSystemProvider;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private Map<String, String> configurationEntries;
    private List<String> configurationResources;
    private HdfsConnectionBuilder hdfsConnectionBuilder;
    private Map<String, String> additionalData;

    @Before
    public void setUp() {
        initializeConfigurationFields();
        initializeHdfsConfigurationBuilder();
        initializeHdfsFileSystemProvider();
    }

    private void initializeConfigurationFields() {
        initializeConfigurationResources();
        initializeConfigurationEntries();
        initializeConfigurationAdditionalData();
    }

    private void initializeConfigurationAdditionalData() {
        additionalData = new HashMap<>();
        additionalData.put("hadoop.security.kerberos.keytab", "hdfs.keytab");
    }

    private void initializeConfigurationEntries() {
        configurationEntries = new HashMap<>();
        configurationEntries.put("fs.defaultFS", "hdfs://localhost:90000");
        configurationEntries.put("hadoop.job.ugi", "nn/localhost@LOCALHOST");
        configurationEntries.put("dfs.namenode.keytab.file", "namenode.keytab");
        configurationEntries.put("dfs.datanode.keytab.file", "datanode.keytab");
    }

    private void initializeConfigurationResources() {
        configurationResources = new ArrayList<>();
        configurationResources.add("configuration-resource-1.xml");
        configurationResources.add("configuration-resource-2.xml");
    }

    private void initializeHdfsFileSystemProvider() {
        hdfsFileSystemProvider = new HdfsFileSystemProvider();
    }

    private void initializeHdfsConfigurationBuilder() {
        hdfsConnectionBuilder = new HdfsConnectionBuilder();
    }

    @Test
    public void fileSystemProvisionUsingHdfsSchemaWhenSimpleAuth() {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth("hdfs://" + CLUSTER_AUTHORITY, null, null, null);
        MuleFileSystem muleFileSystem = hdfsFileSystemProvider.fileSystem(hdfsConnection);
        assertThat(muleFileSystem, notNullValue());
    }

    @Test
    public void fileSystemProvisionUsingSWebHdfsSchemaWhenSimpleAuth() {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth("swebhdfs://" + CLUSTER_AUTHORITY, null, null, null);
        MuleFileSystem muleFileSystem = hdfsFileSystemProvider.fileSystem(hdfsConnection);
        assertThat(muleFileSystem, notNullValue());
    }

    @Test
    public void fileSystemProvisionUsingWebHdfsSchemaWhenSimpleAuth() {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth("webhdfs://" + CLUSTER_AUTHORITY, null, null, null);
        MuleFileSystem muleFileSystem = hdfsFileSystemProvider.fileSystem(hdfsConnection);
        assertThat(muleFileSystem, notNullValue());
    }

    @Test
    public void fileSystemProvisionUsingUnsupportedSchemaWhenSimpleAuth() {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth("http://" + CLUSTER_AUTHORITY, null, null, null);
        expectedException.expect(RuntimeIO.class);
        hdfsFileSystemProvider.fileSystem(hdfsConnection);
    }

    @Test
    public void fileSystemProvisionWhenInvalidURL() {
        HdfsConnection hdfsConnection = hdfsConnectionBuilder.forSimpleAuth("hdfs://fake:9000", null, null, null);
        expectedException.expect(IllegalArgumentException.class);
        hdfsFileSystemProvider.fileSystem(hdfsConnection);
    }

    @Test
    public void hdfsConfigurationBuilderSetsConfigurationEntries() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, Collections.emptyList(), Collections.emptyMap());
        Configuration configuration = new HdfsFileSystemProvider.HdfsConfigurationBuilder().build(hdfsConnection);
        checkConfigurationEntries(configuration, configurationEntries);
    }

    private void checkConfigurationEntries(Configuration configuration, Map<String, String> expectedConfigurationEntries) {
        expectedConfigurationEntries.entrySet().forEach(
                expectedConfigurationEntry -> assertThat(configuration.get(expectedConfigurationEntry.getKey()), is(expectedConfigurationEntry.getValue())));
    }

    @Test
    public void hdfsConfigurationBuilderSetsConfigurationResources() {
        HdfsConnection hdfsConnection = new HdfsConnection(Collections.emptyMap(), configurationResources, Collections.emptyMap());
        Configuration configuration = new HdfsFileSystemProvider.HdfsConfigurationBuilder().build(hdfsConnection);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        checkConfigurationEntries(configuration, expectedConfigurationEntries);
    }

    @Test
    public void hdfsConfigurationBuilderDoesNotThrowExceptionResourcesWhenResourceFileDoesNotExist() {
        List<String> configurationResources = new ArrayList<>();
        configurationResources.add("fileThatDoesNotExist.xml");
        HdfsConnection hdfsConnection = new HdfsConnection(Collections.emptyMap(), configurationResources, Collections.emptyMap());
        new HdfsFileSystemProvider.HdfsConfigurationBuilder().build(hdfsConnection);
    }

    @Test
    public void hdfsConfigurationBuilderDoesNotThrowExceptionResourcesWhenResourceFileHasWrongFormat() {
        List<String> configurationResources = new ArrayList<>();
        configurationResources.add("configuration-resource-wrong-format.xml");
        HdfsConnection hdfsConnection = new HdfsConnection(Collections.emptyMap(), configurationResources, Collections.emptyMap());
        new HdfsFileSystemProvider.HdfsConfigurationBuilder().build(hdfsConnection);
    }
}
