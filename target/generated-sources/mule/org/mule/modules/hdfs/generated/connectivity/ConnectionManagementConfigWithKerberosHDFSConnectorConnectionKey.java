
package org.mule.modules.hdfs.generated.connectivity;

import javax.annotation.Generated;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectionKey;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey implements ConnectionManagementConnectionKey
{

    /**
     * 
     */
    private String nameNodeUri;

    public ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey(String nameNodeUri) {
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
        if (!(o instanceof ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey)) {
            return false;
        }
        ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey that = ((ConnectionManagementConfigWithKerberosHDFSConnectorConnectionKey) o);
        if (((this.nameNodeUri!= null)?(!this.nameNodeUri.equals(that.nameNodeUri)):(that.nameNodeUri!= null))) {
            return false;
        }
        return true;
    }

}
