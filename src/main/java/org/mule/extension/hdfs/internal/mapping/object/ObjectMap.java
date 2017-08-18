/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.mapping.object;

import org.codehaus.jackson.map.ObjectMapper;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;

/**
 * @author MuleSoft, Inc.
 */
public class ObjectMap implements BeanMapper {

    private ObjectMapper mapper = new ObjectMapper();

    @Override public <I, O> O map(I original, Class<O> destClass) {
        return mapper.convertValue(original, destClass);
    }
}
