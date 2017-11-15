/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.exception;

/**
 * @author MuleSoft, Inc.
 */
public class InvalidRequestDataException extends RuntimeException {

    private static final long serialVersionUID = -7589867696620893927L;

    private final String errorCode;

    public InvalidRequestDataException(String message, String errorCode, Exception cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public InvalidRequestDataException(String message) {
        this(message, null, null);
    }

    public String getErrorCode() {
        return errorCode;
    }
}
