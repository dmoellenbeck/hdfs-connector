
package org.mule.modules.hdfs;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.util.StringUtils;

public class HdfsConnectorTestCase extends FunctionalTestCase
{
    @BeforeClass
    public static void setupEnvironment()
    {
        // if no HDFS configuration has been passed as a system property, fallback to
        // local file system
        final String testHdfsFsDefaultName = System.getProperty("test.hdfs.fs.default.name");
        if (StringUtils.isBlank(testHdfsFsDefaultName))
        {
            System.setProperty("test.hdfs.fs.default.name", "file:///");
            System.setProperty("test.hdfs.temp.dir", System.getProperty("java.io.tmpdir"));
        }
    }

    @Override
    protected String getConfigResources()
    {
        return "hdfs-connector-tests-config.xml";
    }

    @Test
    public void fileSystemOperations() throws MuleException
    {
        final String testDirName = "mule-hdfs-test-dir-" + RandomStringUtils.randomAlphanumeric(20);
        final String testFileName = "mule-hdfs-test-file-" + RandomStringUtils.randomAlphanumeric(20)
                                    + ".dat";

        final Map<String, Object> dirNamePathProperties = Collections.<String, Object> singletonMap(
            "hdfsPath", System.getProperty("test.hdfs.temp.dir") + "/" + testDirName);

        // create a directory and assert it exists
        MuleMessage response = muleContext.getClient().send("vm://pathExistsFlow.in", null,
            dirNamePathProperties);
        assertThat(response.getPayload(Boolean.class), is(false));

        muleContext.getClient().send("vm://makeDirectoriesFlow.in", null, dirNamePathProperties);

        response = muleContext.getClient().send("vm://pathExistsFlow.in", null, dirNamePathProperties);
        assertThat(response.getPayload(Boolean.class), is(true));

        // create a file and read it back
        final Map<String, Object> fileNamePathProperties = Collections.<String, Object> singletonMap(
            "hdfsPath", "/tmp/" + testDirName + "/" + testFileName);

        // TODO implement file tests

        // delete a directory and assert its content and itself are gone
        muleContext.getClient().send("vm://deleteDirectoryFlow.in", null, dirNamePathProperties);

        response = muleContext.getClient().send("vm://pathExistsFlow.in", null, dirNamePathProperties);
        assertThat(response.getPayload(Boolean.class), is(false));
        response = muleContext.getClient().send("vm://pathExistsFlow.in", null, fileNamePathProperties);
        assertThat(response.getPayload(Boolean.class), is(false));
    }
}
