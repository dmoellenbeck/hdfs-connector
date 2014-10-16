/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.automation.testcases;

import org.junit.Rule;
import org.junit.rules.Timeout;
import org.mule.modules.tests.ConnectorTestCase;

public class HDFSTestParent extends ConnectorTestCase {

    // Set global timeout of tests to 5 minutes
    @Rule
    public Timeout globalTimeout = new Timeout(300000);


}
