/*
 * $Id: LicenseManager.java 10480 2007-12-19 00:47:04Z moosa $
 * --------------------------------------------------------------------------------------
 * (c) 2003-2008 MuleSource, Inc. This software is protected under international copyright
 * law. All use of this software is subject to MuleSource's Master Subscription Agreement
 * (or other master license agreement) separately entered into in writing between you and
 * MuleSource. If such an agreement is not in place, you may not use the software.
 */

package org.mule.modules.hdfs;

import org.mule.api.annotations.ExpressionEvaluator;
import org.mule.api.annotations.param.Payload;

// FIXME having this uncommented crashes DevKit  
//@ExpressionLanguage(name = "hdfs", minMuleVersion = "3.3")
public class HdfsExpressionLanguage
{
    /**
     * Determine if a particular path exists.
     * 
     * @param expression the expression to be evaluated.
     * @param path the path to be tested.
     * @return true if the provided path exists.
     */
    @ExpressionEvaluator
    public Boolean doesPathExist(final String expression, @Payload final String path)
    {
        // TODO implement whenever codegen works
        System.out.printf("%n%nExpr: %s -> %s%n%n%n", expression, path);
        return true;
    }
}
