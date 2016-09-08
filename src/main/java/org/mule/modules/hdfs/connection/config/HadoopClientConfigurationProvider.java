/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.connection.config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;
import java.util.Map;

/**
 * @author MuleSoft, Inc.
 */
public class HadoopClientConfigurationProvider {

    public Configuration forSimpleAuth(String nameNodeUri, String principal, List<String> configurationResources, Map<String, String> configurationEntries) {
        validateNameNodeUri(nameNodeUri);
        Configuration configuration = newConfiguration(nameNodeUri, principal, configurationResources, configurationEntries);
        return configuration;
    }

    private Configuration newConfiguration(String nameNodeUri, String principal, List<String> configurationResources, Map<String, String> configurationEntries) {
        Configuration configuration = new Configuration();
        configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri);
        if (StringUtils.isNotEmpty(principal)) {
            configuration.set("hadoop.job.ugi", principal);
        }
        if (CollectionUtils.isNotEmpty(configurationResources)) {
            for (String configurationResource : configurationResources) {
                configuration.addResource(configurationResource);
            }
        }
        if (MapUtils.isNotEmpty(configurationEntries)) {
            for (Map.Entry<String, String> configurationEntry : configurationEntries.entrySet()) {
                configuration.set(configurationEntry.getKey(), configurationEntry.getValue());
            }
        }
        return configuration;
    }

    private void validateNameNodeUri(String nameNodeUri) {
        if (StringUtils.isEmpty(nameNodeUri)) {
            throw new IllegalArgumentException("\"nameNodeUri\" can not be null or empty.");
        }
    }

    public Configuration forKerberosAuth(String nameNodeUri, String principal, List<String> configurationResources, Map<String, String> configurationEntries) {
        validateNameNodeUri(nameNodeUri);
        validatePrincipal(principal);

        Configuration configuration = newConfiguration(nameNodeUri, principal, configurationResources, configurationEntries);
        configuration.set("hadoop.security.authentication", "kerberos");
        return configuration;
    }

    private void validatePrincipal(String principal) {
        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("\"principal\" can not be null or empty.");
        }
    }
}
