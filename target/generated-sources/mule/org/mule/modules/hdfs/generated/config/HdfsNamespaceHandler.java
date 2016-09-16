
package org.mule.modules.hdfs.generated.config;

import javax.annotation.Generated;
import org.mule.config.MuleManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;


/**
 * Registers bean definitions parsers for handling elements in <code>http://www.mulesoft.org/schema/mule/hdfs</code>.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.0", date = "2016-09-16T09:46:00-03:00", comments = "Build UNNAMED.2793.f49b6c7")
public class HdfsNamespaceHandler
    extends NamespaceHandlerSupport
{

    private static Logger logger = LoggerFactory.getLogger(HdfsNamespaceHandler.class);

    private void handleException(String beanName, String beanScope, NoClassDefFoundError noClassDefFoundError) {
        String muleVersion = "";
        try {
            muleVersion = MuleManifest.getProductVersion();
        } catch (Exception _x) {
            logger.error("Problem while reading mule version");
        }
        logger.error(((((("Cannot launch the mule app, the  "+ beanScope)+" [")+ beanName)+"] within the connector [hdfs] is not supported in mule ")+ muleVersion));
        throw new FatalBeanException(((((("Cannot launch the mule app, the  "+ beanScope)+" [")+ beanName)+"] within the connector [hdfs] is not supported in mule ")+ muleVersion), noClassDefFoundError);
    }

    /**
     * Invoked by the {@link DefaultBeanDefinitionDocumentReader} after construction but before any custom elements are parsed. 
     * @see NamespaceHandlerSupport#registerBeanDefinitionParser(String, BeanDefinitionParser)
     * 
     */
    public void init() {
        try {
            this.registerBeanDefinitionParser("config-with-kerberos", new HDFSConnectorKerberosConfigDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("config-with-kerberos", "@Config", ex);
        }
        try {
            this.registerBeanDefinitionParser("config", new HDFSConnectorSimpleConfigDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("config", "@Config", ex);
        }
        try {
            this.registerBeanDefinitionParser("read-operation", new ReadOperationDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("read-operation", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("get-metadata", new GetMetadataDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("get-metadata", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("write", new WriteDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("write", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("append", new AppendDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("append", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("delete-file", new DeleteFileDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("delete-file", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("delete-directory", new DeleteDirectoryDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("delete-directory", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("make-directories", new MakeDirectoriesDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("make-directories", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("rename", new RenameDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("rename", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("list-status", new ListStatusDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("list-status", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("glob-status", new GlobStatusDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("glob-status", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("copy-from-local-file", new CopyFromLocalFileDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("copy-from-local-file", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("copy-to-local-file", new CopyToLocalFileDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("copy-to-local-file", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("set-permission", new SetPermissionDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("set-permission", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("set-owner", new SetOwnerDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("set-owner", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("read", new ReadDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("read", "@Source", ex);
        }
    }

}
