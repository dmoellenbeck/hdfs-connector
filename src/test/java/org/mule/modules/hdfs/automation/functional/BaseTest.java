/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;

@Ignore
public class BaseTest extends MuleArtifactFunctionalTestCase{

    private static Properties hdfsProp;

    public static String getNodeNameUri() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_NAME_NODE_URI);
    }

    public static String getUsername() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_USERNAME);
    }

    @BeforeClass
    public static void setUp() {
        try {
            hdfsProp = TestDataBuilder.getProperties(TestDataBuilder.CREDENTIALS_FILE);
        } catch (IOException e) {
            fail("Cannot read property file!");
        }
    }

}
