/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.modules.hdfs.exception.Exception;

import java.io.ByteArrayInputStream;

public class RenameTestCases extends AbstractTestCases {

    private static final String ROOT_DIRECTORY = "rootDirectory/";
    private static final String DIRECTORY = ROOT_DIRECTORY + "directory";
    private static final String NEW_DIRECTORY_NAME = ROOT_DIRECTORY + "newNameDirectory";
    private static final String MYFILE_PATH = ROOT_DIRECTORY + "myfile.txt";
    private static final String NEW_MYFILE_PATH = ROOT_DIRECTORY + "myfile1.txt";
    private byte[] writtenData;
    @Rule
    public ExpectedException fileNotFoundExpected = ExpectedException.none();

    @Before
    public void setUp() throws java.lang.Exception {
        writtenData = TestDataBuilder.payloadForRename();
        getConnector().write(MYFILE_PATH, "700", true, 4096, 1, 1048576, null, null, new ByteArrayInputStream(writtenData));
        getConnector().makeDirectories(DIRECTORY, "700");
    }

    @Test
    public void testRenameDir() throws java.lang.Exception {
        getConnector().rename(DIRECTORY, NEW_DIRECTORY_NAME);
        getConnector().listStatus(NEW_DIRECTORY_NAME, null);
        fileNotFoundExpected.expect(Exception.class);
        getConnector().listStatus(DIRECTORY, null);
    }

    @Test
    public void testRenameFile() throws java.lang.Exception {
        getConnector().rename(MYFILE_PATH, NEW_MYFILE_PATH);
        getConnector().listStatus(NEW_MYFILE_PATH, null);
        fileNotFoundExpected.expect(Exception.class);
        getConnector().listStatus(MYFILE_PATH, null);
    }

    @After
    public void tearDown() throws java.lang.Exception {
        getConnector().deleteDirectory(ROOT_DIRECTORY);
    }
}
