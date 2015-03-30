
package org.mule.modules.hdfs.processors;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Generated;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.config.ConfigurationException;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.registry.RegistrationException;
import org.mule.common.DefaultResult;
import org.mule.common.FailureType;
import org.mule.common.Result;
import org.mule.common.metadata.ConnectorMetaDataEnabled;
import org.mule.common.metadata.DefaultMetaData;
import org.mule.common.metadata.DefaultPojoMetaDataModel;
import org.mule.common.metadata.DefaultSimpleMetaDataModel;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.MetaDataModel;
import org.mule.common.metadata.OperationMetaDataEnabled;
import org.mule.common.metadata.datatype.DataType;
import org.mule.common.metadata.datatype.DataTypeFactory;
import org.mule.devkit.processor.DevkitBasedMessageProcessor;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.security.oauth.callback.ProcessCallback;


/**
 * WriteMessageProcessor invokes the {@link org.mule.modules.hdfs.HDFSConnector#write(java.lang.String, java.lang.String, boolean, int, int, long, java.lang.String, java.lang.String, java.io.InputStream)} method in {@link HDFSConnector }. For each argument there is a field in this processor to match it.  Before invoking the actual method the processor will evaluate and transform where possible to the expected argument type.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.6.1", date = "2015-03-30T02:49:38-03:00", comments = "Build UNNAMED.2405.44720b7")
public class WriteMessageProcessor
    extends DevkitBasedMessageProcessor
    implements MessageProcessor, OperationMetaDataEnabled
{

    protected Object path;
    protected String _pathType;
    protected Object permission;
    protected String _permissionType;
    protected Object overwrite;
    protected boolean _overwriteType;
    protected Object bufferSize;
    protected int _bufferSizeType;
    protected Object replication;
    protected int _replicationType;
    protected Object blockSize;
    protected long _blockSizeType;
    protected Object ownerUserName;
    protected String _ownerUserNameType;
    protected Object ownerGroupName;
    protected String _ownerGroupNameType;
    protected Object payload;
    protected InputStream _payloadType;

    public WriteMessageProcessor(String operationName) {
        super(operationName);
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

    @Override
    public void start()
        throws MuleException
    {
        super.start();
    }

    @Override
    public void stop()
        throws MuleException
    {
        super.stop();
    }

    @Override
    public void dispose() {
        super.dispose();
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
     * Sets replication
     * 
     * @param value Value to set
     */
    public void setReplication(Object value) {
        this.replication = value;
    }

    /**
     * Sets ownerUserName
     * 
     * @param value Value to set
     */
    public void setOwnerUserName(Object value) {
        this.ownerUserName = value;
    }

    /**
     * Sets overwrite
     * 
     * @param value Value to set
     */
    public void setOverwrite(Object value) {
        this.overwrite = value;
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
     * Sets payload
     * 
     * @param value Value to set
     */
    public void setPayload(Object value) {
        this.payload = value;
    }

    /**
     * Sets blockSize
     * 
     * @param value Value to set
     */
    public void setBlockSize(Object value) {
        this.blockSize = value;
    }

    /**
     * Sets permission
     * 
     * @param value Value to set
     */
    public void setPermission(Object value) {
        this.permission = value;
    }

    /**
     * Sets ownerGroupName
     * 
     * @param value Value to set
     */
    public void setOwnerGroupName(Object value) {
        this.ownerGroupName = value;
    }

    /**
     * Invokes the MessageProcessor.
     * 
     * @param event MuleEvent to be processed
     * @throws Exception
     */
    public MuleEvent doProcess(final MuleEvent event)
        throws Exception
    {
        Object moduleObject = null;
        try {
            moduleObject = findOrCreate(null, false, event);
            final String _transformedPath = ((String) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_pathType").getGenericType(), null, path));
            final String _transformedPermission = ((String) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_permissionType").getGenericType(), null, permission));
            final Boolean _transformedOverwrite = ((Boolean) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_overwriteType").getGenericType(), null, overwrite));
            final Integer _transformedBufferSize = ((Integer) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_bufferSizeType").getGenericType(), null, bufferSize));
            final Integer _transformedReplication = ((Integer) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_replicationType").getGenericType(), null, replication));
            final Long _transformedBlockSize = ((Long) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_blockSizeType").getGenericType(), null, blockSize));
            final String _transformedOwnerUserName = ((String) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_ownerUserNameType").getGenericType(), null, ownerUserName));
            final String _transformedOwnerGroupName = ((String) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_ownerGroupNameType").getGenericType(), null, ownerGroupName));
            final InputStream _transformedPayload = ((InputStream) evaluateAndTransform(getMuleContext(), event, WriteMessageProcessor.class.getDeclaredField("_payloadType").getGenericType(), null, payload));
            final ProcessTemplate<Object, Object> processTemplate = ((ProcessAdapter<Object> ) moduleObject).getProcessTemplate();
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
                    ((HDFSConnector) object).write(_transformedPath, _transformedPermission, _transformedOverwrite, _transformedBufferSize, _transformedReplication, _transformedBlockSize, _transformedOwnerUserName, _transformedOwnerGroupName, _transformedPayload);
                    return null;
                }

            }
            , this, event);
            return event;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Result<MetaData> getInputMetaData() {
        return new DefaultResult<MetaData>(new DefaultMetaData(getPojoOrSimpleModel(InputStream.class)));
    }

    @Override
    public Result<MetaData> getOutputMetaData(MetaData inputMetadata) {
        return new DefaultResult<MetaData>(new DefaultMetaData(getPojoOrSimpleModel(void.class)));
    }

    private MetaDataModel getPojoOrSimpleModel(Class clazz) {
        DataType dataType = DataTypeFactory.getInstance().getDataType(clazz);
        if (DataType.POJO.equals(dataType)) {
            return new DefaultPojoMetaDataModel(clazz);
        } else {
            return new DefaultSimpleMetaDataModel(dataType);
        }
    }

    public Result<MetaData> getGenericMetaData(MetaDataKey metaDataKey) {
        ConnectorMetaDataEnabled connector;
        try {
            connector = ((ConnectorMetaDataEnabled) findOrCreate(null, false, null));
            try {
                Result<MetaData> metadata = connector.getMetaData(metaDataKey);
                if ((Result.Status.FAILURE).equals(metadata.getStatus())) {
                    return metadata;
                }
                if (metadata.get() == null) {
                    return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "There was an error processing metadata at HDFSConnector at write retrieving was successful but result is null");
                }
                return metadata;
            } catch (Exception e) {
                return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
            }
        } catch (ClassCastException cast) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "There was an error getting metadata, there was no connection manager available. Maybe you're trying to use metadata from an Oauth connector");
        } catch (ConfigurationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (RegistrationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (IllegalAccessException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (InstantiationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (Exception e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        }
    }

}
