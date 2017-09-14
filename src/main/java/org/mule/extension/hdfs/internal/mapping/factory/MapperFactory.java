/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.mapping.factory;

import org.dozer.DozerBeanMapper;
import org.mule.extension.hdfs.internal.mapping.BeanMapper;
import org.mule.extension.hdfs.internal.mapping.dozer.DozerMapper;
import org.mule.extension.hdfs.internal.mapping.object.ObjectMap;

import java.io.ByteArrayInputStream;

public class MapperFactory {
    
    private MapperFactory(){
    }

    private static final String DOZER_CONFIG_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<mappings xmlns=\"http://dozer.sourceforge.net\"\n"
            + "          xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "          xsi:schemaLocation=\"http://dozer.sourceforge.net\n"
            + "          http://dozer.sourceforge.net/schema/beanmapping.xsd\">\n"
            + "\n"
            + "    <configuration>\n"
            + "    </configuration>\n"
            + "\n"
            + "</mappings>";

    private static final BeanMapper DOZER_MAPPER;

    static {
        DozerBeanMapper dozerBeanMapper = new DozerBeanMapper();
        dozerBeanMapper.addMapping(new ByteArrayInputStream(DOZER_CONFIG_XML.getBytes()));
        DOZER_MAPPER = new DozerMapper(dozerBeanMapper);
    }

    public static BeanMapper dozerMapper() {
        return DOZER_MAPPER;
    }

    public static BeanMapper objectMapperBased() {
        return new ObjectMap();
    }

}
