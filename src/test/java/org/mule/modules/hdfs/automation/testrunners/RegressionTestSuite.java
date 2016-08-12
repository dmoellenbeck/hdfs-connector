/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.automation.testrunners;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.testcases.*;
import org.mule.modules.hdfs.automation.testmetadata.GetMetaDataTestCases;

@RunWith(Categories.class)
@IncludeCategory(RegressionTests.class)
@SuiteClasses({
        AppendTestCases.class,
        CopyFromLocalFileTestCases.class,
        CopyToLocalFileTestCases.class,
        DeleteFileTestCases.class,
        GetMetadataTestCases.class,
        ListStatusTestCases.class,
        GlobStatusTestCases.class,
        MakeDirectoriesTestCases.class,
        ReadTestCases.class,
        RenameTestCases.class,
        SetOwnerTestCases.class,
        SetPermissionTestCases.class,
        WriteTestCases.class,
        DeleteDirectoryTestCases.class,
        GetMetaDataTestCases.class
})
public class RegressionTestSuite {

}
