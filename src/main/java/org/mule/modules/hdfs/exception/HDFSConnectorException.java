/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.exception;

public class HDFSConnectorException extends Exception {


    public HDFSConnectorException() {
        super();
    }

    public HDFSConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public HDFSConnectorException(String message) {
        super(message);
    }

    public HDFSConnectorException(Throwable cause) {
        super(cause);
    }
}
