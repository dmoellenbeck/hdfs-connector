
package org.mule.modules.hdfs.generated.sources;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Generated;
import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.callback.SourceCallback;
import org.mule.api.construct.FlowConstructAware;
import org.mule.api.context.MuleContextAware;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.lifecycle.Startable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.api.source.MessageSource;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.security.oauth.callback.ProcessCallback;
import org.mule.security.oauth.processor.AbstractListeningMessageProcessor;


/**
 * ReadMessageSource wraps {@link org.mule.modules.hdfs.HDFSConnector#read(java.lang.String, int, org.mule.api.callback.SourceCallback)} method in {@link HDFSConnector } as a message source capable of generating Mule events.  The POJO's method is invoked in its own thread.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class ReadMessageSource
    extends AbstractListeningMessageProcessor
    implements Runnable, FlowConstructAware, MuleContextAware, Startable, Stoppable, MessageSource
{

    protected Object path;
    protected String _pathType;
    protected Object bufferSize;
    protected int _bufferSizeType;
    /**
     * Thread under which this message source will execute
     * 
     */
    private java.lang.Thread thread;
    /**
     * Polling Period for SourceStrategy.POLLING
     * 
     */
    static Long pollingPeriod;

    public ReadMessageSource(String operationName) {
        super(operationName);
    }

    /**
     * Sets pollingPeriod
     * 
     * @param value Value to set
     */
    public void setPollingPeriod(Long value) {
        this.pollingPeriod = value;
    }

    /**
     * Obtains the expression manager from the Mule context and initialises the connector. If a target object  has not been set already it will search the Mule registry for a default one.
     * 
     * @throws InitialisationException
     */
    public void initialise()
        throws InitialisationException
    {
    }

    /**
     * Sets bufferSize
     * 
     * @param value Value to set
     */
    public void setBufferSize(Object value) {
        this.bufferSize = value;
    }

    /**
     * Sets path
     * 
     * @param value Value to set
     */
    public void setPath(Object value) {
        this.path = value;
    }

    /**
     * Method to be called when Mule instance gets started.
     * 
     */
    public void start()
        throws MuleException
    {
        if (thread == null) {
            thread = new java.lang.Thread(this, "Receiving Thread");
        }
        thread.start();
    }

    /**
     * Method to be called when Mule instance gets stopped.
     * 
     */
    public void stop()
        throws MuleException
    {
        thread.interrupt();
    }

    /**
     * Implementation {@link Runnable#run()} that will invoke the method on the pojo that this message source wraps.
     * 
     */
    public void run() {
        Object moduleObject = null;
        try {
            moduleObject = findOrCreate(null, false, null);
            final ProcessTemplate<Object, Object> processTemplate = ((ProcessAdapter<Object> ) moduleObject).getProcessTemplate();
            final SourceCallback sourceCallback = this;
            final String transformedPath = ((String) transform(getMuleContext(), ((MuleEvent) null), getClass().getDeclaredField("_pathType").getGenericType(), null, path));
            final Integer transformedBufferSize = ((Integer) transform(getMuleContext(), ((MuleEvent) null), getClass().getDeclaredField("_bufferSizeType").getGenericType(), null, bufferSize));
            while (!Thread.currentThread().isInterrupted()) {
                processTemplate.execute(new ProcessCallback<Object,Object>() {


                    public List<Class<? extends Exception>> getManagedExceptions() {
                        return Arrays.asList(((Class<? extends Exception> []) new Class[] {IOException.class }));
                    }

                    public boolean isProtected() {
                        return false;
                    }

                    public Object process(Object object)
                        throws Exception
                    {
                        ((HDFSConnector) object).read(transformedPath, transformedBufferSize, sourceCallback);
                        return null;
                    }

                }
                , null, ((MuleEvent) null));
                Thread.currentThread().sleep(pollingPeriod);
            }
        } catch (MessagingException e) {
            getFlowConstruct().getExceptionListener().handleException(e, e.getEvent());
        } catch (Exception e) {
            getMuleContext().getExceptionListener().handleException(e);
        }
    }

}
