/**
     * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
     */

package org.mule.extension.hdfs.automation.functional;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.tck.junit4.rule.SystemProperty;
import org.mule.tck.util.TestConnectivityUtils;
import org.mule.test.runner.ArtifactClassLoaderRunnerConfig;

@ArtifactClassLoaderRunnerConfig(testInclusions = {"org.apache.hadoop:hadoop-client", "commons-lang:commons-lang"})
public abstract class BaseTest extends MuleArtifactFunctionalTestCase {

    @Rule
    public SystemProperty disableAutomaticTestConnectivity = TestConnectivityUtils.disableAutomaticTestConnectivity();

    private static Properties hdfsProp;

    public static String getNodeNameUri() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_NAME_NODE_URI);
    }

    public static String getUsername() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_USERNAME);
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        try {
            hdfsProp = TestDataBuilder.getProperties(TestDataBuilder.CREDENTIALS_FILE);
        } catch (IOException e) {
            fail("Cannot read property file!");
        }
    }

}
