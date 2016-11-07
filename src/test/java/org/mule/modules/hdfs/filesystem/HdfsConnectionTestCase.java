/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsConnectionTestCase {

    private Map<String, String> configurationEntries;
    private List<String> configurationResources;
    private Map<String, String> additionalData;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        initializeConfigurationEntries();
        initializeConfigurationResources();
        initializeAdditionalData();
    }

    private void initializeConfigurationEntries() {
        configurationEntries = new HashMap<>();
        configurationEntries.put("hadoop.security.authentication", "kerberos");
    }

    private void initializeConfigurationResources() {
        configurationResources = new ArrayList<>();
        configurationResources.add("configuration-resources-1.xml");
        configurationResources.add("configuration-resources-2.xml");
    }

    private void initializeAdditionalData() {
        additionalData = new HashMap<>();
        additionalData.put("hadoop.security.kerberos.keytab", "hdfs.keytab");
    }

    @Test
    public void instantiation() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, configurationResources, additionalData);
        validateDataThatWereSet(hdfsConnection, configurationEntries, configurationResources, additionalData);
    }

    private void validateDataThatWereSet(HdfsConnection hdfsConnection,
            Map<String, String> configurationEntries, List<String> configurationResources, Map<String, String> additionalData) {
        assertThat(hdfsConnection.getConfigurationEntries(), equalTo(configurationEntries));
        assertThat(hdfsConnection.getConfigurationResources(), equalTo(configurationResources));
        assertThat(hdfsConnection.getAdditionalData(), equalTo(additionalData));
    }

    @Test
    public void instantiationWhenConfigurationEntriesIsNull() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Configuration entries can not be null");
        new HdfsConnection(null, configurationResources, additionalData);
    }

    @Test
    public void instantiationWhenConfigurationEntriesIsEmpty() {
        HdfsConnection hdfsConnection = new HdfsConnection(Collections.emptyMap(), configurationResources, additionalData);
        validateDataThatWereSet(hdfsConnection, Collections.emptyMap(), configurationResources, additionalData);
    }

    @Test
    public void instantiationWhenConfigurationResourcesIsNull() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Configuration resources can not be null");
        new HdfsConnection(configurationEntries, null, additionalData);
    }

    @Test
    public void instantiationWhenConfigurationResourcesIsEmpty() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, Collections.emptyList(), additionalData);
        validateDataThatWereSet(hdfsConnection, configurationEntries, Collections.emptyList(), additionalData);
    }

    @Test
    public void instantiationWhenAdditionalDataIsNull() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Additional data can not be null");
        new HdfsConnection(configurationEntries, configurationResources, null);
    }

    @Test
    public void instantiationWhenAdditionalDataIsEmpty() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, configurationResources, Collections.emptyMap());
        validateDataThatWereSet(hdfsConnection, configurationEntries, configurationResources, Collections.emptyMap());
    }

    @Test
    public void immutabilityOfConfigurationEntries() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, configurationResources, additionalData);
        expectedException.expect(UnsupportedOperationException.class);
        hdfsConnection.getConfigurationEntries()
                .put("extra", "extra");
    }

    @Test
    public void immutabilityOfConfigurationResources() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, configurationResources, additionalData);
        expectedException.expect(UnsupportedOperationException.class);
        hdfsConnection.getConfigurationResources()
                .add("extra");
    }

    @Test
    public void immutabilityOfAdditionalData() {
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, configurationResources, additionalData);
        expectedException.expect(UnsupportedOperationException.class);
        hdfsConnection.getAdditionalData()
                .put("extra", "extra");
    }
}
