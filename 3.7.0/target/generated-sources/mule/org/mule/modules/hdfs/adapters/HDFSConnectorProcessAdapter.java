
package org.mule.modules.hdfs.adapters;

import javax.annotation.Generated;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.routing.filter.Filter;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.security.oauth.callback.ProcessCallback;


/**
 * A <code>HDFSConnectorProcessAdapter</code> is a wrapper around {@link HDFSConnector } that enables custom processing strategies.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.6.1", date = "2015-03-30T02:49:38-03:00", comments = "Build UNNAMED.2405.44720b7")
public class HDFSConnectorProcessAdapter
    extends HDFSConnectorLifecycleInjectionAdapter
    implements ProcessAdapter<HDFSConnectorCapabilitiesAdapter>
{


    public<P >ProcessTemplate<P, HDFSConnectorCapabilitiesAdapter> getProcessTemplate() {
        final HDFSConnectorCapabilitiesAdapter object = this;
        return new ProcessTemplate<P,HDFSConnectorCapabilitiesAdapter>() {


            @Override
            public P execute(ProcessCallback<P, HDFSConnectorCapabilitiesAdapter> processCallback, MessageProcessor messageProcessor, MuleEvent event)
                throws Exception
            {
                return processCallback.process(object);
            }

            @Override
            public P execute(ProcessCallback<P, HDFSConnectorCapabilitiesAdapter> processCallback, Filter filter, MuleMessage message)
                throws Exception
            {
                return processCallback.process(object);
            }

        }
        ;
    }

}
