/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.exception;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

public class ExceptionMessages {

    private ExceptionMessages() {

    }

    // exception messages
    private static final String UNABLE_RETRIEVE = "Something went wrong while retrieving data from Hadoop";
    private static final String UNABLE_SEND = "Something went wrong while sending data to Hadoop: ";
    private static final String INVALID_REQUEST_DATA = "Invalid request data :";
    private static final String ILLEGAL_ARGUMENT = "Invalid parameter!";
    
    private static final String INVALID_INPUT_FILTER = "Invalid input filter: ";
    private static final String INVALID_FILE_PATH = "Invalid file path: ";

    private static final String UNKNOWN_EXCEPTION_MESSAGE = "Unknown error occurred!";

    static Map<String, String> exceptionsMapping = new HashMap<>();

    static {
        exceptionsMapping.put(HdfsConnectionException.class.getSimpleName(), UNABLE_SEND);
        exceptionsMapping.put(UnableToRetrieveResponseException.class.getSimpleName(), UNABLE_RETRIEVE);
        exceptionsMapping.put(InvalidRequestDataException.class.getSimpleName(), INVALID_REQUEST_DATA);
        exceptionsMapping.put(IllegalArgumentException.class.getSimpleName(), ILLEGAL_ARGUMENT);
        exceptionsMapping.put(PatternSyntaxException.class.getSimpleName(), INVALID_INPUT_FILTER);
        exceptionsMapping.put(FileNotFoundException.class.getSimpleName(), INVALID_FILE_PATH);

    }
    

    public static String resolveExceptionMessage(String exceptionClass) {
        if (exceptionsMapping.containsKey(exceptionClass)) {
            return exceptionsMapping.get(exceptionClass);
        }
        return UNKNOWN_EXCEPTION_MESSAGE;
    }
}