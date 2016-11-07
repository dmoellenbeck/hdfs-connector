/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.enricher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.introspection.exception.ExceptionEnricher;

public class IOExceptionException implements ExceptionEnricher {

    private List<? extends Class> connectionExceptions = Arrays.asList(IOException.class);

    @Override
    public Exception enrichException(Exception exception) {
        Iterator<? extends Class> iterator = connectionExceptions.iterator();
        while (iterator.hasNext()) {
            if (iterator.next()
                    .isInstance(exception)) {
                return new ConnectionException(exception);
            }
        }
        return exception;
    }

}
