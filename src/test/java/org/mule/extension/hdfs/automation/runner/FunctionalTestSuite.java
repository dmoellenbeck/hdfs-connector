/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.automation.runner;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.extension.hdfs.automation.functional.ReadOperationTestCases;
import org.mule.extension.hdfs.automation.functional.WriteTestCases;

@RunWith(Suite.class)
@SuiteClasses({
        ReadOperationTestCases.class,
        WriteTestCases.class,

})
public class FunctionalTestSuite {

}
