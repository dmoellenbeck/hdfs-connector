/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.exception;

/**
 * @author MuleSoft, Inc.
 */
public class RuntimeIO extends RuntimeException {

    private static final long serialVersionUID = -6946769863174144082L;

    public RuntimeIO(String message, Throwable cause) {
        super(message, cause);
    }
}
