/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.util;

public class TestConstants {

    public static final String READ_OPERATION_FLOW_PATH = "flows/readOp-flows.xml";
    public static final String WRITE_OPERATION_FLOW_PATH = "flows/write-flows.xml";
    public static final String LIST_STATUS_FLOW_PATH = "flows/listStatus-flows.xml";
    public static final String GLOBAL_STATUS_FLOW_PATH = "flows/globalStatus-flows.xml";
    public static final String DELETE_DIR_FLOW_PATH = "flows/delDirectory-flows.xml";
    public static final String MAKE_DIR_FLOW_PATH = "flows/delDirectory-flows.xml";
    public static final String GET_METADATA_FLOW_PATH = "flows/getMetadata-flows.xml";
    public static final String APPEND_FLOW_PATH = "flows/append-flows.xml";
    public static final String DELETE_FILE_FLOW_PATH = "flows/delFile-flows.xml";
    public static final String RENAME_FLOW_PATH = "flows/rename-flows.xml";
    public static final String COPY_TO_LOCAL_FLOW_PATH = "flows/copyToLocal-flows.xml";
    public static final String COPY_FROM_LOCAL_FLOW_PATH = "flows/copyFromLocal-flows.xml";
    public static final String SET_PERMISSION_FLOW_PATH = "flows/setPermission-flows.xml";


    public class FlowNames {

        public static final String READ_OP_FLOW = "readOpFlow";
        public static final String WRITE_FLOW = "writeFlow";
        public static final String LIST_STATUS_FLOW = "listStatusFlow";
        public static final String GLOB_STATUS_FLOW = "globStatusFlow";
        public static final String MAKE_DIR_FLOW = "makeDirFlow";
        public static final String DELETE_DIR_FLOW = "deleteDirFlow";
        public static final String GET_METADATA_FLOW = "getMetadataFlow";
        public static final String APPEND_DIR_FLOW = "appendFlow";
        public static final String DELETE_FILE_FLOW = "deleteFileFlow";
        public static final String RENAME_FLOW = "renameFlow";
        public static final String COPY_TO_LOCAL_FILE_FLOW = "copyToLocalFileFlow";
        public static final String COPY_FROM_LOCAL_FILE_FLOW = "copyFromLocalFileFlow";
        public static final String SET_PERMISSION_FLOW = "setPermissionFlow";

    }

    public class ExceptionMessages {
        public static final String UNABLE_RETRIEVE = "Something went wrong while retrieving data from Hadoop";
        public static final String UNABLE_SEND = "Something went wrong while sending data to Hadoop: ";
        public static final String INVALID_REQUEST_DATA = "Invalid request data :";
        public static final String ILLEGAL_ARGUMENT = "Invalid parameter!";
        public static final String ILLEGAL_PATH = "Can not create a Path";
        public static final String INVALID_INPUT_FILTER = "Invalid input filter: ";
        public static final String INVALID_FILE_PATH = "Invalid file path: ";
        public static final String UNKNOWN_EXCEPTION_MESSAGE = "Unknown error occurred!";
    }
}