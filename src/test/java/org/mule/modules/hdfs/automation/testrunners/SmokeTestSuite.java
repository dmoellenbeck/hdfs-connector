/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testrunners;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.SmokeTests;
import org.mule.modules.hdfs.automation.testcases.*;
import org.mule.modules.hdfs.automation.testmetadata.GetMetaDataTestCases;

@RunWith(Categories.class)
@IncludeCategory(SmokeTests.class)
@SuiteClasses({
        AppendTestCases.class,
        DeleteFileTestCases.class,
        GetMetadataTestCases.class,
        MakeDirectoriesTestCases.class,
        ReadTestCases.class,
        WriteTestCases.class,
        DeleteDirectoryTestCases.class,
        GetMetaDataTestCases.class
})
public class SmokeTestSuite {
}
