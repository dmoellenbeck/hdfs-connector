/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.exception;

/**
 * @author MuleSoft, Inc.
 */
public class UnableToUnsubscribeException extends RuntimeException {

    private static final long serialVersionUID = -8510324073626637265L;

    public UnableToUnsubscribeException(String message, Throwable cause) {
        super(message, cause);
    }
}
