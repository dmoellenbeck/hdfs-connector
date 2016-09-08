/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.unit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.mule.modules.hdfs.connection.config.HadoopClientConfigurationProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class HadoopClientConfigurationProviderTest {

    private HadoopClientConfigurationProvider provider;
    private String nameNodeUri;
    private String principal;
    private List<String> configurationResources;
    private Map<String, String> configurationEntries;
    private Map<String, String> configurationEntriesFromResourceFile;

    @Rule
    public ExpectedException exceptionValidator = ExpectedException.none();

    @Before
    public void setUp() {
        initializeConfigurationFields();
        provider = new HadoopClientConfigurationProvider();
    }

    private void initializeConfigurationFields() {
        nameNodeUri = "hdfs://localhost:90000";
        principal = "nn/localhost@LOCALHOST";
        configurationResources = new ArrayList<>();
        configurationResources.add("configuration-resource-1.xml");
        configurationResources.add("configuration-resource-2.xml");
        configurationEntries = new HashMap<>();
        configurationEntries.put("dfs.namenode.keytab.file", "namenode.keytab");
        configurationEntries.put("dfs.datanode.keytab.file", "datanode.keytab");
        configurationEntriesFromResourceFile = new HashMap<>();
        configurationEntriesFromResourceFile.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
    }

    @Test
    public void testForSimpleAuth() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    private void validateConfigurationForSimpleAuth(Configuration configuration) {
        assertThat(configuration, notNullValue());
    }

    private void validateThatConfigurationContainsProperties(Configuration configuration, Map<String, String> configurationEntries) {
        for (Map.Entry<String, String> configurationEntry : configurationEntries.entrySet()) {
            assertThat(configuration.get(configurationEntry.getKey()), is(configurationEntry.getValue()));
        }
    }

    @Test
    public void testForSimpleAuthWhenNameNodeUriIsNull() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forSimpleAuth(null, principal, configurationResources, configurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenNameNodeUriIsEmpty() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forSimpleAuth("", principal, configurationResources, configurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenPrincipalIsNull() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, null, configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenPrincipalIsEmpty() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, "", configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenConfigurationResourcesIsNull() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, principal, null, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenConfigurationResourcesIsEmpty() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, principal, new ArrayList<String>(), configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenConfigurationEntriesIsNull() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, null);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForSimpleAuthWhenConfigurationEntriesIsEmpty() {
        Configuration configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, new HashMap<String, String>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForKerberosAuth() {
        Configuration configuration = provider.forKerberosAuth(nameNodeUri, principal, configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    private void validateConfigurationForKerberosAuth(Configuration configuration) {
        assertThat(configuration, notNullValue());
        assertThat(configuration.get("hadoop.security.authentication"), is("kerberos"));
    }

    @Test
    public void testForKerberosAuthWhenNameNodeUriIsNull() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(null, principal, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenNameNodeUriIsEmpty() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth("", principal, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenPrincipalIsNull() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(nameNodeUri, null, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenPrincipalIsEmpty() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(nameNodeUri, "", configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationResourcesIsNull() {
        Configuration configuration = provider.forKerberosAuth(nameNodeUri, principal, null, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationResourcesIsEmpty() {
        Configuration configuration = provider.forKerberosAuth(nameNodeUri, principal, new ArrayList<String>(), configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationEntriesIsNull() {
        Configuration configuration = provider.forKerberosAuth(nameNodeUri, principal, configurationResources, null);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationEntriesIsEmpty() {
        Configuration configuration = provider.forKerberosAuth(nameNodeUri, principal, configurationResources, new HashMap<String, String>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.auth_to_local", "RULE:[1:$1@$0](hdfs-Sandbox@HDP.SANDBOX)s/.*/hdfs/");
        expectedConfigurationEntries.put("dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST");
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
    }

}
