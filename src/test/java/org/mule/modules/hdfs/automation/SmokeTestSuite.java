/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.modules.hdfs.automation;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.testcases.DeleteDirectoryTestCases;
import org.mule.modules.hdfs.automation.testcases.DeleteFileTestCases;
import org.mule.modules.hdfs.automation.testcases.GetMetadataTestCases;
import org.mule.modules.hdfs.automation.testcases.MakeDirectoriesTestCases;
import org.mule.modules.hdfs.automation.testcases.SmokeTests;
import org.mule.modules.hdfs.automation.testcases.WriteTestCases;

@RunWith(Categories.class)
@IncludeCategory(SmokeTests.class)
@SuiteClasses({
	DeleteDirectoryTestCases.class,
	DeleteFileTestCases.class,
	GetMetadataTestCases.class,
	MakeDirectoriesTestCases.class,
	WriteTestCases.class
})
public class SmokeTestSuite {
}
