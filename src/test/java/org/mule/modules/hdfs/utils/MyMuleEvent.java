///**
// * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.hdfs.utils;
//
//import org.mule.MessageExchangePattern;
//import org.mule.api.*;
//import org.mule.api.construct.FlowConstruct;
//import org.mule.api.context.notification.FlowCallStack;
//import org.mule.api.context.notification.ProcessorsTrace;
//import org.mule.api.security.Credentials;
//import org.mule.api.transformer.DataType;
//import org.mule.api.transformer.TransformerException;
//import org.mule.api.transport.ReplyToHandler;
//import org.mule.management.stats.ProcessingTime;
//
//import java.io.OutputStream;
//import java.io.Serializable;
//import java.net.URI;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//
//public class MyMuleEvent implements MuleEvent {
//
//    private static final long serialVersionUID = 1L;
//
//    private Map<String, Object> flowVars;
//    private Map<String, Object> sessionVars;
//
//    public MyMuleEvent() {
//        flowVars = new HashMap<String, Object>();
//        sessionVars = new HashMap<String, Object>();
//    }
//
//    @Override
//    public void captureReplyToDestination() {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void clearFlowVariables() {
//        flowVars.clear();
//    }
//
//    @Override
//    public void clearSessionVariables() {
//        sessionVars.clear();
//    }
//
//    @Override
//    public boolean isNotificationsEnabled() {
//        return false; // To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public void setEnableNotifications(boolean b) {
//        // To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public boolean isAllowNonBlocking() {
//        return false;
//    }
//
//    @Override
//    public FlowCallStack getFlowCallStack() {
//        return null;
//    }
//
//    @Override
//    public ProcessorsTrace getProcessorsTrace() {
//        return null;
//    }
//
//    @Override
//    public Credentials getCredentials() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String getEncoding() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public MessageExchangePattern getExchangePattern() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public FlowConstruct getFlowConstruct() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public <T> T getFlowVariable(String arg0) {
//        return (T) flowVars.get(arg0);
//    }
//
//    @Override
//    public DataType<?> getFlowVariableDataType(String key) {
//        return null;
//    }
//
//    @Override
//    public Set<String> getFlowVariableNames() {
//        return flowVars.keySet();
//    }
//
//    @Override
//    public String getId() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public MuleMessage getMessage() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public void setMessage(MuleMessage arg0) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public byte[] getMessageAsBytes() throws MuleException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String getMessageAsString() throws MuleException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String getMessageAsString(String arg0) throws MuleException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String getMessageSourceName() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public URI getMessageSourceURI() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public MuleContext getMuleContext() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public OutputStream getOutputStream() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public ProcessingTime getProcessingTime() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    @Deprecated
//    public Object getProperty(String arg0) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    @Deprecated
//    public Object getProperty(String arg0, Object arg1) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public Object getReplyToDestination() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public ReplyToHandler getReplyToHandler() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public MuleSession getSession() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public <T> T getSessionVariable(String arg0) {
//        return (T) sessionVars.get(arg0);
//    }
//
//    @Override
//    public DataType<?> getSessionVariableDataType(String key) {
//        return null;
//    }
//
//    @Override
//    public Set<String> getSessionVariableNames() {
//        return sessionVars.keySet();
//    }
//
//    @Override
//    public int getTimeout() {
//        // TODO Auto-generated method stub
//        return 0;
//    }
//
//    @Override
//    public void setTimeout(int arg0) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean isStopFurtherProcessing() {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public void setStopFurtherProcessing(boolean arg0) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean isSynchronous() {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean isTransacted() {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public void removeFlowVariable(String arg0) {
//        flowVars.remove(arg0);
//    }
//
//    @Override
//    public void removeSessionVariable(String arg0) {
//        flowVars.remove(arg0);
//    }
//
//    @Override
//    public void setFlowVariable(String arg0, Object arg1) {
//        flowVars.put(arg0, arg1);
//    }
//
//    @Override
//    public void setFlowVariable(String key, Object value, DataType dataType) {
//
//    }
//
//    @Override
//    public void setSessionVariable(String arg0, Object arg1) {
//        sessionVars.put(arg0, arg1);
//    }
//
//    @Override
//    public void setSessionVariable(String key, Serializable value, DataType dataType) {
//
//    }
//
//    @Override
//    @Deprecated
//    public Object transformMessage() throws TransformerException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public <T> T transformMessage(Class<T> arg0) throws TransformerException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public <T> T transformMessage(DataType<T> arg0) throws TransformerException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    @Deprecated
//    public byte[] transformMessageToBytes() throws TransformerException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String transformMessageToString() throws TransformerException {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//}
