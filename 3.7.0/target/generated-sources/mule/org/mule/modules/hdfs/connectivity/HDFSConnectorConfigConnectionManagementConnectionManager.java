
package org.mule.modules.hdfs.connectivity;

import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import org.apache.commons.pool.KeyedObjectPool;
import org.mule.api.MetadataAware;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.config.MuleProperties;
import org.mule.api.context.MuleContextAware;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.devkit.capability.Capabilities;
import org.mule.api.devkit.capability.ModuleCapability;
import org.mule.api.lifecycle.Disposable;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.retry.RetryPolicyTemplate;
import org.mule.common.DefaultTestResult;
import org.mule.common.TestResult;
import org.mule.common.Testable;
import org.mule.config.PoolingProfile;
import org.mule.devkit.processor.ExpressionEvaluatorSupport;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectionAdapter;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectionManager;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectorAdapter;
import org.mule.devkit.shade.connection.management.ConnectionManagementConnectorFactory;
import org.mule.devkit.shade.connection.management.ConnectionManagementProcessTemplate;
import org.mule.devkit.shade.connection.management.UnableToAcquireConnectionException;
import org.mule.devkit.shade.connectivity.ConnectivityTestingErrorHandler;
import org.mule.modules.hdfs.HDFSConnector;
import org.mule.modules.hdfs.adapters.HDFSConnectorConnectionManagementAdapter;
import org.mule.modules.hdfs.connection.strategy.HDFSConnectionManagement;
import org.mule.modules.hdfs.pooling.DevkitGenericKeyedObjectPool;


/**
 * A {@code HDFSConnectorConfigConnectionManagementConnectionManager} is a wrapper around {@link HDFSConnector } that adds connection management capabilities to the pojo.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.6.1", date = "2015-03-30T02:49:38-03:00", comments = "Build UNNAMED.2405.44720b7")
public class HDFSConnectorConfigConnectionManagementConnectionManager
    extends ExpressionEvaluatorSupport
    implements MetadataAware, MuleContextAware, ProcessAdapter<HDFSConnectorConnectionManagementAdapter> , Capabilities, Disposable, Initialisable, Testable, ConnectionManagementConnectionManager<ConnectionManagementConfigHDFSConnectorConnectionKey, HDFSConnectorConnectionManagementAdapter, HDFSConnectionManagement>
{

    /**
     * 
     */
    private String nameNodeUri;
    private String username;
    private List<String> configurationResources;
    private Map<String, String> configurationEntries;
    /**
     * Mule Context
     * 
     */
    protected MuleContext muleContext;
    /**
     * Connector Pool
     * 
     */
    private KeyedObjectPool connectionPool;
    protected PoolingProfile poolingProfile;
    protected RetryPolicyTemplate retryPolicyTemplate;
    private final static String MODULE_NAME = "HDFS";
    private final static String MODULE_VERSION = "4.0.0";
    private final static String DEVKIT_VERSION = "3.6.1";
    private final static String DEVKIT_BUILD = "UNNAMED.2405.44720b7";
    private final static String MIN_MULE_VERSION = "3.6.0";

    /**
     * Sets nameNodeUri
     * 
     * @param value Value to set
     */
    public void setNameNodeUri(String value) {
        this.nameNodeUri = value;
    }

    /**
     * Retrieves nameNodeUri
     * 
     */
    public String getNameNodeUri() {
        return this.nameNodeUri;
    }

    /**
     * Sets username
     * 
     * @param value Value to set
     */
    public void setUsername(String value) {
        this.username = value;
    }

    /**
     * Retrieves username
     * 
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Sets configurationResources
     * 
     * @param value Value to set
     */
    public void setConfigurationResources(List<String> value) {
        this.configurationResources = value;
    }

    /**
     * Retrieves configurationResources
     * 
     */
    public List<String> getConfigurationResources() {
        return this.configurationResources;
    }

    /**
     * Sets configurationEntries
     * 
     * @param value Value to set
     */
    public void setConfigurationEntries(Map<String, String> value) {
        this.configurationEntries = value;
    }

    /**
     * Retrieves configurationEntries
     * 
     */
    public Map<String, String> getConfigurationEntries() {
        return this.configurationEntries;
    }

    /**
     * Sets muleContext
     * 
     * @param value Value to set
     */
    public void setMuleContext(MuleContext value) {
        this.muleContext = value;
    }

    /**
     * Retrieves muleContext
     * 
     */
    public MuleContext getMuleContext() {
        return this.muleContext;
    }

    /**
     * Sets poolingProfile
     * 
     * @param value Value to set
     */
    public void setPoolingProfile(PoolingProfile value) {
        this.poolingProfile = value;
    }

    /**
     * Retrieves poolingProfile
     * 
     */
    public PoolingProfile getPoolingProfile() {
        return this.poolingProfile;
    }

    /**
     * Sets retryPolicyTemplate
     * 
     * @param value Value to set
     */
    public void setRetryPolicyTemplate(RetryPolicyTemplate value) {
        this.retryPolicyTemplate = value;
    }

    /**
     * Retrieves retryPolicyTemplate
     * 
     */
    public RetryPolicyTemplate getRetryPolicyTemplate() {
        return this.retryPolicyTemplate;
    }

    public void initialise() {
        connectionPool = new DevkitGenericKeyedObjectPool(new ConnectionManagementConnectorFactory(this), poolingProfile);
        if (retryPolicyTemplate == null) {
            retryPolicyTemplate = muleContext.getRegistry().lookupObject(MuleProperties.OBJECT_DEFAULT_RETRY_POLICY_TEMPLATE);
        }
    }

    @Override
    public void dispose() {
        try {
            connectionPool.close();
        } catch (Exception e) {
        }
    }

    public HDFSConnectorConnectionManagementAdapter acquireConnection(ConnectionManagementConfigHDFSConnectorConnectionKey key)
        throws Exception
    {
        return ((HDFSConnectorConnectionManagementAdapter) connectionPool.borrowObject(key));
    }

    public void releaseConnection(ConnectionManagementConfigHDFSConnectorConnectionKey key, HDFSConnectorConnectionManagementAdapter connection)
        throws Exception
    {
        connectionPool.returnObject(key, connection);
    }

    public void destroyConnection(ConnectionManagementConfigHDFSConnectorConnectionKey key, HDFSConnectorConnectionManagementAdapter connection)
        throws Exception
    {
        connectionPool.invalidateObject(key, connection);
    }

    /**
     * Returns true if this module implements such capability
     * 
     */
    public boolean isCapableOf(ModuleCapability capability) {
        if (capability == ModuleCapability.LIFECYCLE_CAPABLE) {
            return true;
        }
        if (capability == ModuleCapability.CONNECTION_MANAGEMENT_CAPABLE) {
            return true;
        }
        return false;
    }

    @Override
    public<P >ProcessTemplate<P, HDFSConnectorConnectionManagementAdapter> getProcessTemplate() {
        return new ConnectionManagementProcessTemplate(this, muleContext);
    }

    @Override
    public ConnectionManagementConfigHDFSConnectorConnectionKey getDefaultConnectionKey() {
        return new ConnectionManagementConfigHDFSConnectorConnectionKey(getNameNodeUri());
    }

    @Override
    public ConnectionManagementConfigHDFSConnectorConnectionKey getEvaluatedConnectionKey(MuleEvent event)
        throws Exception
    {
        if (event!= null) {
            final String _transformedNameNodeUri = ((String) evaluateAndTransform(muleContext, event, this.getClass().getDeclaredField("nameNodeUri").getGenericType(), null, getNameNodeUri()));
            if (_transformedNameNodeUri == null) {
                throw new UnableToAcquireConnectionException("Parameter nameNodeUri in method connect can't be null because is not @Optional");
            }
            return new ConnectionManagementConfigHDFSConnectorConnectionKey(_transformedNameNodeUri);
        }
        return getDefaultConnectionKey();
    }

    public String getModuleName() {
        return MODULE_NAME;
    }

    public String getModuleVersion() {
        return MODULE_VERSION;
    }

    public String getDevkitVersion() {
        return DEVKIT_VERSION;
    }

    public String getDevkitBuild() {
        return DEVKIT_BUILD;
    }

    public String getMinMuleVersion() {
        return MIN_MULE_VERSION;
    }

    @Override
    public ConnectionManagementConfigHDFSConnectorConnectionKey getConnectionKey(MessageProcessor messageProcessor, MuleEvent event)
        throws Exception
    {
        return getEvaluatedConnectionKey(event);
    }

    @Override
    public ConnectionManagementConnectionAdapter newConnection() {
        HDFSConnectionManagementHDFSConnectorAdapter connection = new HDFSConnectionManagementHDFSConnectorAdapter();
        connection.setUsername(getUsername());
        connection.setConfigurationResources(getConfigurationResources());
        connection.setConfigurationEntries(getConfigurationEntries());
        return connection;
    }

    @Override
    public ConnectionManagementConnectorAdapter newConnector(ConnectionManagementConnectionAdapter<HDFSConnectionManagement, ConnectionManagementConfigHDFSConnectorConnectionKey> connection) {
        HDFSConnectorConnectionManagementAdapter connector = new HDFSConnectorConnectionManagementAdapter();
        connector.setConnection(connection.getStrategy());
        return connector;
    }

    public ConnectionManagementConnectionAdapter getConnectionAdapter(ConnectionManagementConnectorAdapter adapter) {
        HDFSConnectorConnectionManagementAdapter connector = ((HDFSConnectorConnectionManagementAdapter) adapter);
        ConnectionManagementConnectionAdapter strategy = ((ConnectionManagementConnectionAdapter) connector.getConnection());
        return strategy;
    }

    public TestResult test() {
        HDFSConnectorConnectionManagementAdapter connection = null;
        DefaultTestResult result;
        ConnectionManagementConfigHDFSConnectorConnectionKey key = getDefaultConnectionKey();
        try {
            connection = acquireConnection(key);
            result = new DefaultTestResult(org.mule.common.Result.Status.SUCCESS);
        } catch (Exception e) {
            try {
                destroyConnection(key, connection);
            } catch (Exception ie) {
            }
            result = ((DefaultTestResult) ConnectivityTestingErrorHandler.buildFailureTestResult(e));
        } finally {
            if (connection!= null) {
                try {
                    releaseConnection(key, connection);
                } catch (Exception ie) {
                }
            }
        }
        return result;
    }

}
