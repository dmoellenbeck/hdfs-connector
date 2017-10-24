/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.internal.message.InternalEvent;


public class TestDataBuilder {

    public static final String CREDENTIALS_FILE = "automation-credentials.properties";
    public static final String HDFS_NAME_NODE_URI = "hdfs.nameNodeUri";
    public static final String HDFS_USERNAME = "hdfs.username";

    public static Properties getProperties(String fileName) throws IOException {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = TestDataBuilder.class.getClassLoader()
                    .getResourceAsStream(fileName);
            prop.load(input);
            return prop;
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static List<String> fileNamesForListStatus() {
        return Arrays.asList("file1.csv", "file2.csv", "file3.csv");
    }

    public static List<String> fileNamesForGlobStatus() {
        return Arrays.asList("2013/12/30", "2013/12/31", "2014/03/20", "2014/04/22");
    }

    public static InputStream payloadForWrite() {
        return new ByteArrayInputStream(RandomStringUtils.randomAlphanumeric(20)
                .getBytes());

    }

    public static Object getValue(CoreEvent ev) {
        return ev.getMessage()
                .getPayload()
                .getValue();
    }

    public static byte[] payloadShortString() throws Exception {
        return RandomStringUtils.randomAlphanumeric(5)
                .getBytes();
    }

}
