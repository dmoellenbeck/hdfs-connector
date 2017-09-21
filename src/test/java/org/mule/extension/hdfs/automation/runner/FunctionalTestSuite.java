/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.runner;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.extension.hdfs.automation.functional.AppendTestCases;
import org.mule.extension.hdfs.automation.functional.CopyFromLocalFileTestCases;
import org.mule.extension.hdfs.automation.functional.CopyToLocalFileTestCases;
import org.mule.extension.hdfs.automation.functional.DeleteDirectoryTestCases;
import org.mule.extension.hdfs.automation.functional.DeleteFileTestCases;
import org.mule.extension.hdfs.automation.functional.GetMetadataTestCases;
import org.mule.extension.hdfs.automation.functional.GlobStatusTestCases;
import org.mule.extension.hdfs.automation.functional.ListStatusTestCases;
import org.mule.extension.hdfs.automation.functional.MakeDirectoriesTestCases;
import org.mule.extension.hdfs.automation.functional.ReadOperationTestCases;
import org.mule.extension.hdfs.automation.functional.RenameTestCases;
import org.mule.extension.hdfs.automation.functional.SetOwnerTestCases;
import org.mule.extension.hdfs.automation.functional.SetPermissionTestCases;
import org.mule.extension.hdfs.automation.functional.WriteTestCases;

@RunWith(Suite.class)
@SuiteClasses({
        AppendTestCases.class,
        CopyFromLocalFileTestCases.class,
        CopyToLocalFileTestCases.class,
        DeleteDirectoryTestCases.class,
        DeleteFileTestCases.class,
        GetMetadataTestCases.class,
        GlobStatusTestCases.class,
        ListStatusTestCases.class,
        MakeDirectoriesTestCases.class,
        ReadOperationTestCases.class,
        RenameTestCases.class,
        SetOwnerTestCases.class,
        SetPermissionTestCases.class,
        WriteTestCases.class
})
public class FunctionalTestSuite {

}
