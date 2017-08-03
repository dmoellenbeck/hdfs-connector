/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.exception;

/**
 * @author MuleSoft, Inc.
 */
public class UnableToRetrieveResponseException extends RuntimeException {

    private static final long serialVersionUID = -5287778333075797450L;

    public UnableToRetrieveResponseException(String message) {
        super(message);
    }

    public UnableToRetrieveResponseException(String message, Exception cause) {
        super(message, cause);
    }
}
