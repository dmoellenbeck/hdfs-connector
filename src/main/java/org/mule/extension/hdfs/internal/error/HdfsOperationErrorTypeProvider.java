/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.error;

import java.util.Set;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

import com.google.common.collect.ImmutableSet;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsOperationErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        return ImmutableSet.<ErrorTypeDefinition> builder()
                .add(HdfsErrorType.INVALID_STRUCTURE_FOR_INPUT_DATA)
                .add(HdfsErrorType.INVALID_REQUEST_DATA)
                .add(HdfsErrorType.CONNECTIVITY)
                .add(HdfsErrorType.UNKNOWN)
                .build();
    }
}
