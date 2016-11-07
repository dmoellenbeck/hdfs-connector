/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.runner;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.functional.*;

@RunWith(Suite.class)
@SuiteClasses({
        AppendTestCases.class,
        CopyFromLocalFileTestCases.class,
        CopyToLocalFileTestCases.class,
        DeleteFileTestCases.class,
        ListStatusTestCases.class,
        GlobStatusTestCases.class,
        MakeDirectoriesTestCases.class
})
public class FunctionalTestSuite {

    // @BeforeClass
    // public static void initialiseSuite() {
    // ConnectorTestContext.initialize(HDFSConnector.class);
    // }

    // @AfterClass
    // public static void shutdownSuite() throws Exception {
    // ConnectorTestContext.shutDown();
    // }
}
