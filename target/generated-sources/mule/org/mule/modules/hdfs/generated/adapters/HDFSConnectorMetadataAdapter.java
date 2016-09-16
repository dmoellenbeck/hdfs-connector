
package org.mule.modules.hdfs.generated.adapters;

import javax.annotation.Generated;
import org.mule.api.MetadataAware;
import org.mule.modules.hdfs.HDFSConnector;


/**
 * A <code>HDFSConnectorMetadataAdapter</code> is a wrapper around {@link HDFSConnector } that adds support for querying metadata about the extension.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class HDFSConnectorMetadataAdapter
    extends HDFSConnectorCapabilitiesAdapter
    implements MetadataAware
{

    private final static String MODULE_NAME = "HDFS";
    private final static String MODULE_VERSION = "5.0.0";
    private final static String DEVKIT_VERSION = "3.9.0";
    private final static String DEVKIT_BUILD = "UNNAMED.2793.f49b6c7";
    private final static String MIN_MULE_VERSION = "3.6.0";

    public String getModuleName() {
        return MODULE_NAME;
    }

    public String getModuleVersion() {
        return MODULE_VERSION;
    }

    public String getDevkitVersion() {
        return DEVKIT_VERSION;
    }

    public String getDevkitBuild() {
        return DEVKIT_BUILD;
    }

    public String getMinMuleVersion() {
        return MIN_MULE_VERSION;
    }

}
