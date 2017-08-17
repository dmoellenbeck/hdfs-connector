/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.mapping.dozer;

import org.dozer.Mapper;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;

/**
 * @author MuleSoft, Inc.
 */
public class DozerMapper implements BeanMapper {

    private Mapper mapper;

    public DozerMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    @Override public <I, O> O map(I original, Class<O> destClass) {
        return mapper.map(original, destClass);
    }
}
