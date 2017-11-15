/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.error;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.runtime.extension.api.error.MuleErrors;

/**
 * @author MuleSoft, Inc.
 */
public enum HdfsErrorType implements ErrorTypeDefinition<HdfsErrorType> {

    CONNECTIVITY(MuleErrors.CONNECTIVITY),

    INVALID_REQUEST_DATA(MuleErrors.ANY),

    INVALID_STRUCTURE_FOR_INPUT_DATA(MuleErrors.TRANSFORMATION),

    UNABLE_TO_UNSUBSCRIBE(MuleErrors.SOURCE),

    UNKNOWN(MuleErrors.ANY);

    private ErrorTypeDefinition<? extends Enum<?>> parent;

    HdfsErrorType(ErrorTypeDefinition<? extends Enum<?>> parent) {
        this.parent = parent;
    }
}
