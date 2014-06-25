/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testrunners;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.RegressionTests;
import org.mule.modules.hdfs.automation.testcases.*;

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
        SetPermissionTestCases.class,
        WriteTestCases.class,
        DeleteDirectoryTestCases.class
})
public class RegressionTestSuite {

}
