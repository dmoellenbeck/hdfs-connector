/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsConnectionBuilder {

    public HdfsConnection forSimpleAuth(String nameNodeUri, String principal, List<String> configurationResources, Map<String, String> configurationEntries) {
        validateNameNodeUri(nameNodeUri);
        return newConfiguration(nameNodeUri, principal, configurationResources, configurationEntries, Collections.emptyMap());
    }

    private HdfsConnection newConfiguration(String nameNodeUri, String principal, List<String> configurationResources, Map<String, String> configurationEntries,
            Map<String, String> additionalData) {
        Map<String, String> enrichedConfigurationEntries = new HashMap<>();

        enrichedConfigurationEntries.put("fs.defaultFS", nameNodeUri);
        if (StringUtils.isNotEmpty(principal)) {
            enrichedConfigurationEntries.put("hadoop.job.ugi", principal);
        }

        List<String> enrichedConfigurationResources = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(configurationResources)) {
            enrichedConfigurationResources.addAll(configurationResources);
        }

        if (MapUtils.isNotEmpty(configurationEntries)) {
            enrichedConfigurationEntries.putAll(configurationEntries);
        }

        return new HdfsConnection(enrichedConfigurationEntries, enrichedConfigurationResources, additionalData);
    }

    private void validateNameNodeUri(String nameNodeUri) {
        if (StringUtils.isEmpty(nameNodeUri)) {
            throw new IllegalArgumentException("\"nameNodeUri\" can not be null or empty.");
        }
    }

    public HdfsConnection forKerberosAuth(String nameNodeUri, String principal, String keytabPath,
            List<String> configurationResources, Map<String, String> configurationEntries) {
        validateNameNodeUri(nameNodeUri);
        validatePrincipal(principal);

        Map<String, String> enrichedMap = new HashMap<>();
        enrichedMap.put("hadoop.security.authentication", "kerberos");

        if (MapUtils.isNotEmpty(configurationEntries)) {
            enrichedMap.putAll(configurationEntries);
        }

        Map<String, String> additionalData = new HashMap<>();
        if (StringUtils.isNotEmpty(keytabPath)) {
            additionalData.put("hadoop.security.kerberos.keytab", keytabPath);
        }
        HdfsConnection configuration = newConfiguration(nameNodeUri, principal, configurationResources, enrichedMap, additionalData);

        return configuration;
    }

    private void validatePrincipal(String principal) {
        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("\"principal\" can not be null or empty.");
        }
    }
}
