/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.system;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mule.api.ConnectionException;
import org.mule.modules.hdfs.automation.runner.SystemTestsFilterByAutomationCredentialsProperties;
import org.mule.modules.hdfs.connection.config.Kerberos;
import org.mule.tools.devkit.ctf.configuration.util.ConfigurationUtils;

import java.util.Properties;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(SystemTestsFilterByAutomationCredentialsProperties.class)
public class KerberosConfigTestCases {

    public static final String FAKE_NAME_NODE_URI = "fakeNameNodeURI";
    public static final String KEYTAB_THAT_DOES_NOT_EXIST_KEYTAB = "keytabThatDoesNotExist.keytab";
    private Kerberos config;
    private Properties configuration;
    @Rule
    public ExpectedException exceptionEvaluator = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        configuration = loadConfigurationProperties();
        initializeConnectorConfig();
    }

    private void initializeConnectorConfig() throws Exception {
        config = new Kerberos();
        config.setUsername(configuration.getProperty("config-with-kerberos.username"));
        config.setKeytabPath(configuration.getProperty("config-with-kerberos.keytabPath"));
    }

    @NotNull
    private Properties loadConfigurationProperties() throws Exception {
        return ConfigurationUtils.getAutomationCredentialsProperties();
    }

    @Test
    public void testConnect() throws Exception {
        config.connect(configuration.getProperty("config-with-kerberos.nameNodeUri"));
    }

    @Test
    public void testConnectWhenInvalidNameNodeUri() throws Exception {
        exceptionEvaluator.expect(IllegalArgumentException.class);
        config.connect(FAKE_NAME_NODE_URI);
    }

    @Test
    public void testConnectWhenUsernameIsNull() throws Exception {
        config.setUsername(null);
        exceptionEvaluator.expect(IllegalArgumentException.class);
        config.connect(configuration.getProperty("config-with-kerberos.nameNodeUri"));
    }

    @Test
    public void testConnectWhenUsernameIsEmpty() throws Exception {
        config.setUsername("");
        exceptionEvaluator.expect(IllegalArgumentException.class);
        config.connect(configuration.getProperty("config-with-kerberos.nameNodeUri"));
    }

    @Test
    public void testConnectWhenInvalidKeytab() throws Exception {
        config.setKeytabPath(KEYTAB_THAT_DOES_NOT_EXIST_KEYTAB);
        exceptionEvaluator.expect(ConnectionException.class);
        config.connect(configuration.getProperty("config-with-kerberos.nameNodeUri"));
    }
}
