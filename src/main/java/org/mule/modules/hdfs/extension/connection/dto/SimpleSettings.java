/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.dto;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

/**
 * @author MuleSoft, Inc.
 */
public class SimpleSettings {

    /**
     * A simple user identity of a client process. It is passed to HDFS client as the "hadoop.job.ugi" configuration entry. It can be overriden by values in configurationResources
     * and configurationEntries.
     *
     */
    @Optional
    private String username;
    /**
     *
     * @param nameNodeUri
     *            The name of the file system to connect to. It is passed to HDFS client as the {FileSystem#FS_DEFAULT_NAME_KEY} configuration entry. It can be overriden by values
     *            in configurationResources and configurationEntries.
     */
    @DisplayName("NameNode URI")
    public String nameNodeUri;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(String nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }
}
