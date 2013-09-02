package org.mule.modules.hdfs.automation.testcases;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.mule.api.MuleEvent;
import org.mule.api.processor.MessageProcessor;
import org.mule.tck.junit4.FunctionalTestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class HDFSTestParent extends FunctionalTestCase {
	
	// Set global timeout of tests to 10minutes
    @Rule
    public Timeout globalTimeout = new Timeout(600000);

	protected static final String[] SPRING_CONFIG_FILES = new String[] { "AutomationSpringBeans.xml" };
	protected static ApplicationContext context;
	protected Map<String, Object> testObjects;
	
	@Override
	protected String getConfigResources() {
		return "automation-test-flows.xml";
	}

	protected MessageProcessor lookupFlowConstruct(String name) {
		return (MessageProcessor) muleContext.getRegistry().lookupFlowConstruct(name);
	}
	
	@BeforeClass
	public static void beforeClass() {
		context = new ClassPathXmlApplicationContext(SPRING_CONFIG_FILES);
	}
	
	/*
	 * Helper method below
	 */
	
	public void deleteFile(String path) throws Exception {
		testObjects.put("path", path);
		
		MessageProcessor flow = lookupFlowConstruct("delete-file");
		MuleEvent response = flow.process(getTestEvent(testObjects));
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
