package org.mule.modules.hdfs.automation;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.hdfs.automation.testcases.AppendTestCases;
import org.mule.modules.hdfs.automation.testcases.DeleteDirectoryTestCases;
import org.mule.modules.hdfs.automation.testcases.DeleteFileTestCases;
import org.mule.modules.hdfs.automation.testcases.GetMetadataTestCases;
import org.mule.modules.hdfs.automation.testcases.MakeDirectoriesTestCases;
import org.mule.modules.hdfs.automation.testcases.ReadTestCases;
import org.mule.modules.hdfs.automation.testcases.WriteTestCases;

@RunWith(Categories.class)
@IncludeCategory(RegressionTestSuite.class)
@SuiteClasses({
	AppendTestCases.class,
	DeleteDirectoryTestCases.class,
	DeleteFileTestCases.class,
	GetMetadataTestCases.class,
	MakeDirectoriesTestCases.class,
	ReadTestCases.class,
	WriteTestCases.class
})
public class RegressionTestSuite {

}
