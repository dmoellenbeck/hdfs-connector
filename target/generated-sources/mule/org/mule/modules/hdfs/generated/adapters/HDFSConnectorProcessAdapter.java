
package org.mule.modules.hdfs.generated.adapters;

import javax.annotation.Generated;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.routing.filter.Filter;
import org.mule.devkit.internal.lic.LicenseValidatorFactory;
import org.mule.devkit.internal.lic.validator.LicenseValidator;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.security.oauth.callback.ProcessCallback;


/**
 * A <code>HDFSConnectorProcessAdapter</code> is a wrapper around {@link HDFSConnector } that enables custom processing strategies.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class HDFSConnectorProcessAdapter
    extends HDFSConnectorLifecycleInjectionAdapter
    implements ProcessAdapter<HDFSConnectorCapabilitiesAdapter> , Initialisable
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

    @Override
    public void initialise()
        throws InitialisationException
    {
        super.initialise();
        checkMuleLicense();
    }

    /**
     * Obtains the expression manager from the Mule context and initialises the connector. If a target object  has not been set already it will search the Mule registry for a default one.
     * 
     * @throws InitialisationException
     */
    private void checkMuleLicense() {
        LicenseValidator licenseValidator = LicenseValidatorFactory.getValidator("HDFS");
        licenseValidator.checkEnterpriseLicense(true);
    }

}
