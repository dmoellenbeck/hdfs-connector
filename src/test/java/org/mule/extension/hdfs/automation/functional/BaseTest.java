/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.extension.hdfs.automation.functional;

import static java.io.File.separator;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.mule.extension.hdfs.util.TestDataBuilder;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.test.runner.ArtifactClassLoaderRunnerConfig;
import org.mule.test.runner.RunnerDelegateTo;

@ArtifactClassLoaderRunnerConfig(testInclusions = {
        "org.apache.hadoop:hadoop-client",
        "commons-lang:commons-lang"
})
@Ignore
@RunnerDelegateTo(Parameterized.class)
public abstract class BaseTest extends MuleArtifactFunctionalTestCase {

    private static Properties hdfsProp;

    public static String getNodeNameUri() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_NAME_NODE_URI);
    }

    public static String getUsername() {
        return hdfsProp.getProperty(TestDataBuilder.HDFS_USERNAME);
    }

    private final String configuration;

    public BaseTest(String configuration) {
        this.configuration = configuration;
    }

    @Override
    protected String[] getConfigFiles() {
        return Stream.concat(Stream.of(getFlowFile()), Stream.of(format("flows/configs/%s-config.xml", configuration), "flows/base-flows.xml"))
                .map(this::replaceSlashes)
                .collect(toList())
                .toArray(new String[]{});
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<String> data() {
        return Stream.of(System.getProperty("activeconfiguration", "simple").split(","))
                .map(String::trim)
                .collect(toList());
    }

    protected String[] getFlowFiles() {
        return new String[]{getFlowFile()};
    }

    protected String getFlowFile() {
        return "";
    }

    private String replaceSlashes(String input) {
        return input.replace("/", separator)
                .replace("\\", separator);
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
