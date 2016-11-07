/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class HdfsConnectionBuilderTest {

    private HdfsConnectionBuilder provider;
    private String nameNodeUri;
    private String principal;
    private String keytabPath;
    private List<String> configurationResources;
    private Map<String, String> configurationEntries;
    @Rule
    public ExpectedException exceptionValidator = ExpectedException.none();

    @Before
    public void setUp() {
        initializeConfigurationFields();
        provider = new HdfsConnectionBuilder();
    }

    private void initializeConfigurationFields() {
        nameNodeUri = "hdfs://localhost:90000";
        principal = "nn/localhost@LOCALHOST";
        keytabPath = "hdfs.keytab";
        initializeConfigurationResources();
        initializeConfigurationEntries();
    }

    private void initializeConfigurationEntries() {
        configurationEntries = new HashMap<>();
        configurationEntries.put("dfs.namenode.keytab.file", "namenode.keytab");
        configurationEntries.put("dfs.datanode.keytab.file", "datanode.keytab");
    }

    private void initializeConfigurationResources() {
        configurationResources = new ArrayList<>();
        configurationResources.add("configuration-resource-1.xml");
        configurationResources.add("configuration-resource-2.xml");
    }

    @Test
    public void testForSimpleAuth() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    private void validateThatConfigurationContainsResources(HdfsConnection configuration, List<String> expectedConfigurationResources) {
        assertThat(configuration.getConfigurationResources(), equalTo(expectedConfigurationResources));
    }

    private void validateConfigurationForSimpleAuth(HdfsConnection configuration) {
        assertThat(configuration, notNullValue());
    }

    private void validateThatConfigurationContainsProperties(HdfsConnection configuration, Map<String, String> expectedConfigurationEntries) {
        assertThat(configuration.getConfigurationEntries(), equalTo(expectedConfigurationEntries));
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
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, null, configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForSimpleAuthWhenPrincipalIsEmpty() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, "", configurationResources, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForSimpleAuthWhenConfigurationResourcesIsNull() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, principal, null, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, Collections.emptyList());
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForSimpleAuthWhenConfigurationResourcesIsEmpty() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, principal, new ArrayList<String>(), configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, Collections.emptyList());
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForSimpleAuthWhenConfigurationEntriesIsNull() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, null);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForSimpleAuthWhenConfigurationEntriesIsEmpty() {
        HdfsConnection configuration = provider.forSimpleAuth(nameNodeUri, principal, configurationResources, new HashMap<String, String>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForSimpleAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForKerberosAuth() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, keytabPath, configurationResources, configurationEntries);
        validateConfigurationForKerberosAuth(configuration);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        Map<String, String> expectedKerberosAdditionalData = new HashMap<>();
        expectedKerberosAdditionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        validateThatConfigurationContainsAdditionalData(configuration, expectedKerberosAdditionalData);
    }

    private void validateThatConfigurationContainsAdditionalData(HdfsConnection configuration, Map<String, String> expectedAdditionalData) {
        assertThat(configuration.getAdditionalData(), equalTo(expectedAdditionalData));
    }

    private void validateConfigurationForKerberosAuth(HdfsConnection configuration) {
        assertThat(configuration, notNullValue());
    }

    @Test
    public void testForKerberosAuthWhenNameNodeUriIsNull() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(null, principal, keytabPath, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenNameNodeUriIsEmpty() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth("", principal, keytabPath, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenPrincipalIsNull() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(nameNodeUri, null, keytabPath, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenPrincipalIsEmpty() {
        exceptionValidator.expect(IllegalArgumentException.class);
        provider.forKerberosAuth(nameNodeUri, "", keytabPath, configurationResources, configurationEntries);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationResourcesIsNull() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, keytabPath, null, configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, Collections.emptyList());
        Map<String, String> expectedKerberosAdditionalData = new HashMap<>();
        expectedKerberosAdditionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        validateThatConfigurationContainsAdditionalData(configuration, expectedKerberosAdditionalData);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationResourcesIsEmpty() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, keytabPath, new ArrayList<>(), configurationEntries);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.putAll(configurationEntries);
        expectedConfigurationEntries.put(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, Collections.emptyList());
        Map<String, String> expectedKerberosAdditionalData = new HashMap<>();
        expectedKerberosAdditionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        validateThatConfigurationContainsAdditionalData(configuration, expectedKerberosAdditionalData);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationEntriesIsNull() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, keytabPath, configurationResources, null);
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        Map<String, String> expectedKerberosAdditionalData = new HashMap<>();
        expectedKerberosAdditionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        validateThatConfigurationContainsAdditionalData(configuration, expectedKerberosAdditionalData);
    }

    @Test
    public void testForKerberosAuthWhenConfigurationEntriesIsEmpty() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, keytabPath, configurationResources, new HashMap<>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        Map<String, String> expectedKerberosAdditionalData = new HashMap<>();
        expectedKerberosAdditionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        validateThatConfigurationContainsAdditionalData(configuration, expectedKerberosAdditionalData);
    }

    @Test
    public void testForKerberosAuthWhenKeytabPathIsEmpty() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, "", configurationResources, new HashMap<>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

    @Test
    public void testForKerberosAuthWhenKeytabPathIsNull() {
        HdfsConnection configuration = provider.forKerberosAuth(nameNodeUri, principal, null, configurationResources, new HashMap<>());
        Map<String, String> expectedConfigurationEntries = new HashMap<>();
        expectedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        expectedConfigurationEntries.put("hadoop.security.authentication", "kerberos");
        expectedConfigurationEntries.put("hadoop.job.ugi", principal);
        validateConfigurationForKerberosAuth(configuration);
        validateThatConfigurationContainsProperties(configuration, expectedConfigurationEntries);
        validateThatConfigurationContainsResources(configuration, configurationResources);
        validateThatConfigurationContainsAdditionalData(configuration, Collections.emptyMap());
    }

}
