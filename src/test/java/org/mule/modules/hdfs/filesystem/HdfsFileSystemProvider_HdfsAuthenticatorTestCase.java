/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(UserGroupInformation.class)
@PowerMockIgnore("javax.management.*")
public class HdfsFileSystemProvider_HdfsAuthenticatorTestCase extends HdfsClusterDependentTestBase {

    private Map<String, String> configurationEntries;
    private Map<String, String> additionalData;

    @Before
    public void setUp() {
        initializeConfigurationFields();
    }

    private void initializeConfigurationFields() {
        initializeConfigurationEntries();
        initializeConfigurationAdditionalData();
    }

    private void initializeConfigurationAdditionalData() {
        additionalData = new HashMap<>();
        additionalData.put("hadoop.security.kerberos.keytab", "hdfs.keytab");
    }

    private void initializeConfigurationEntries() {
        configurationEntries = new HashMap<>();
        configurationEntries.put("hadoop.job.ugi", "nn/localhost@LOCALHOST");
        configurationEntries.put("hadoop.security.authentication", "kerberos");
    }

    @Test
    public void hdfsAuthenticatorUsesKerberosWithKeytab() throws Exception {
        mockUserGroupInformation();
        HdfsConnection hdfsConnection = new HdfsConnection(configurationEntries, Collections.emptyList(), additionalData);
        Configuration configuration = new HdfsFileSystemProvider.HdfsConfigurationBuilder().build(hdfsConnection);
        HdfsFileSystemProvider.HdfsAuthenticator.authenticate(configuration, hdfsConnection);

        PowerMockito.verifyStatic();
        UserGroupInformation.setConfiguration(Mockito.any(Configuration.class));
        PowerMockito.verifyStatic();
        UserGroupInformation.loginUserFromKeytab(Mockito.anyString(), Mockito.anyString());
    }

    private void mockUserGroupInformation() {
        PowerMockito.mockStatic(UserGroupInformation.class);
    }
}
