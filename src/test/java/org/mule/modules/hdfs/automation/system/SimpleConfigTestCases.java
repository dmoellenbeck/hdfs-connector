///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.automation.system;
//
//import org.jetbrains.annotations.NotNull;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.ExpectedException;
//import org.junit.runner.RunWith;
//import org.mule.modules.hdfs.automation.runner.SystemTestsFilterByAutomationCredentialsProperties;
//import org.mule.modules.hdfs.connection.config.Simple;
//import org.mule.tools.devkit.ctf.configuration.util.ConfigurationUtils;
//
//import java.util.Properties;
//
///**
// * @author MuleSoft, Inc.
// */
//@RunWith(SystemTestsFilterByAutomationCredentialsProperties.class)
//public class SimpleConfigTestCases {
//
//    public static final String FAKE_NAME_NODE_URI = "fakeNameNodeURI";
//    private Simple config;
//    private Properties configuration;
//    @Rule
//    public ExpectedException exceptionEvaluator = ExpectedException.none();
//
//    @Before
//    public void setUp() throws Exception {
//        configuration = loadConfigurationProperties();
//        initializeConnectorConfig();
//    }
//
//    private void initializeConnectorConfig() throws Exception {
//        config = new Simple();
//        config.setUsername(configuration.getProperty("config.username"));
//    }
//
//    @NotNull
//    private Properties loadConfigurationProperties() throws Exception {
//        return ConfigurationUtils.getAutomationCredentialsProperties();
//    }
//
//    @Test
//    public void testConnect() throws Exception {
//        config.connect(configuration.getProperty("config.nameNodeUri"));
//    }
//
//    @Test
//    public void testConnectWhenInvalidNameNodeUri() throws Exception {
//        exceptionEvaluator.expect(IllegalArgumentException.class);
//        config.connect(FAKE_NAME_NODE_URI);
//    }
//}
