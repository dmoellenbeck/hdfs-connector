/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.runner;

import org.apache.commons.lang.StringUtils;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.mule.modules.hdfs.automation.system.KerberosConfigTestCases;
import org.mule.modules.hdfs.automation.system.SimpleConfigTestCases;
import org.mule.tools.devkit.ctf.configuration.util.ConfigurationUtils;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * @author MuleSoft, Inc.
 */
public class SystemTestsFilterByAutomationCredentialsProperties extends BlockJUnit4ClassRunner {

    private final Properties automationCredentials;

    public SystemTestsFilterByAutomationCredentialsProperties(Class<?> klass) throws InvocationTargetException, InitializationError, ConfigurationLoadingFailedException {
        super(klass);
        automationCredentials = ConfigurationUtils.getAutomationCredentialsProperties();
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        boolean shouldIRun = false;

        if (StringUtils.isNotEmpty(automationCredentials.getProperty("config-with-kerberos.username")) &&
                description.getClassName()
                        .equals(KerberosConfigTestCases.class.getName())) {
            shouldIRun = true;
        } else if (StringUtils.isNotEmpty(automationCredentials.getProperty("config.nameNodeUri")) &&
                description.getClassName()
                        .equals(SimpleConfigTestCases.class.getName())) {
            shouldIRun = true;
        }

        if (!shouldIRun) {
            notifier.fireTestIgnored(description);
        } else {
            super.runChild(method, notifier);
        }
    }
}
