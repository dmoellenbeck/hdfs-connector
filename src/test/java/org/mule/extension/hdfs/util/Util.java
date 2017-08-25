/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.util;

public class Util {

    public static final String READ_OPERATION_FLOW_PATH = "flows/readOp-flows.xml";
    public static final String WRITE_OPERATION_FLOW_PATH = "flows/write-flows.xml";
    public static final String LIST_STATUS_FLOW_PATH = "flows/listStatus-flows.xml";
    public static final String GLOBAL_STATUS_FLOW_PATH = "flows/globalStatus-flows.xml";
    public static final String DELETE_DIR_FLOW_PATH = "flows/delDirectory-flows.xml";
    public static final String MAKE_DIR_FLOW_PATH = "flows/delDirectory-flows.xml";

    public class FlowNames {

        public static final String READ_OP_FLOW = "readOpFlow";
        public static final String WRITE_FLOW = "writeFlow";
        public static final String LIST_STATUS_FLOW = "listStatusFlow";
        public static final String GLOB_STATUS_FLOW = "globStatusFlow";
        public static final String MAKE_DIR_FLOW = "makeDirFlow";
        public static final String DELETE_DIR_FLOW = "deleteDirFlow";
        public static final String GET_METADATA_FLOW = "getMetadataFlow";
     }

}