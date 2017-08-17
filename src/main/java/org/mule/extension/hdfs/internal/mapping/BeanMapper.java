/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.mapping;


/**
 * @author MuleSoft, Inc.
 */
public interface BeanMapper {

    <I, O> O map(I original, Class<O> destClass);

}
