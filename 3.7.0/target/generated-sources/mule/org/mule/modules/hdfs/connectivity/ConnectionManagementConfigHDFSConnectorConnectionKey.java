
package org.mule.modules.hdfs.connectivity;

import javax.annotation.Generated;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectionKey;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.6.1", date = "2015-03-30T02:49:38-03:00", comments = "Build UNNAMED.2405.44720b7")
public class ConnectionManagementConfigHDFSConnectorConnectionKey implements ConnectionManagementConnectionKey
{

    /**
     * 
     */
    private String nameNodeUri;

    public ConnectionManagementConfigHDFSConnectorConnectionKey(String nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }

    /**
     * Sets nameNodeUri
     * 
     * @param value Value to set
     */
    public void setNameNodeUri(String value) {
        this.nameNodeUri = value;
    }

    /**
     * Retrieves nameNodeUri
     * 
     */
    public String getNameNodeUri() {
        return this.nameNodeUri;
    }

    @Override
    public int hashCode() {
        int result = ((this.nameNodeUri!= null)?this.nameNodeUri.hashCode(): 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionManagementConfigHDFSConnectorConnectionKey)) {
            return false;
        }
        ConnectionManagementConfigHDFSConnectorConnectionKey that = ((ConnectionManagementConfigHDFSConnectorConnectionKey) o);
        if (((this.nameNodeUri!= null)?(!this.nameNodeUri.equals(that.nameNodeUri)):(that.nameNodeUri!= null))) {
            return false;
        }
        return true;
    }

}
