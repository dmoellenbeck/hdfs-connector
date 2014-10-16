/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is
 * published under the terms of the CPAL v1.0 license, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.hdfs.integration;

import org.apache.commons.io.IOUtils;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.callback.SourceCallback;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;

public class MySourceCallback implements SourceCallback {

    @Override
    public Object process() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object process(Object arg0) throws Exception {
        return null;
    }

    @Override
    public Object process(Object arg0, Map<String, Object> arg1)
            throws Exception {
        if (arg0 instanceof InputStream) {
            InputStream inputStream = (InputStream) arg0;
            StringWriter writer = new StringWriter();
            IOUtils.copy(inputStream, writer, "UTF-8");
            inputStream.close();
            return writer.toString();
        }
        return null;
    }

    @Override
    public MuleEvent processEvent(MuleEvent arg0) throws MuleException {
        // TODO Auto-generated method stub
        return null;
    }
}
