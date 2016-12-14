/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.mule.modules.hdfs.extension.connection.dto.AdvancedSettings;
import org.mule.modules.hdfs.extension.connection.dto.SimpleSettings;
import org.mule.modules.hdfs.filesystem.*;
import org.mule.modules.hdfs.filesystem.exception.AuthenticationFailed;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionExceptionCode;
import org.mule.runtime.api.connection.ConnectionValidationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleTestCase {

    private static final String VALID_NAME_NODE_URI = "hdfs://localhost:9000";
    private static final String VALID_USERNAME = "principal";
    private static final List<String> VALID_CONFIGURATION_RESOURCES = new ArrayList<>();
    private static final Map<String, String> VALID_CONFIGURATION_ENTRIES = new HashMap<>();
    private static final String INVALID_NAME_NODE_URI = "hdfs://invalid:9000";
    private static final String INVALID_USERNAME = "invalid";
    private static final List<String> INVALID_CONFIGURATION_RESOURCES = new ArrayList<>();
    private static final Map<String, String> INVALID_CONFIGURATION_ENTRIES = new HashMap<>();
    @Mock
    private SimpleSettings validSimpleSettings;
    @Mock
    private AdvancedSettings validAdvancedSettings;
    @Mock
    private SimpleSettings invalidSimpleSettings;
    @Mock
    private AdvancedSettings invalidAdvancedSettings;
    @Mock
    private HdfsConnection validConnection;
    @Mock
    private HdfsConnection wrongConnection;
    @Mock
    private HdfsFileSystemProvider hdfsFileSystemProvider;
    @Mock
    private HdfsConnectionBuilder hdfsConnectionBuilder;
    @Mock
    private HdfsFileSystem hdfsFileSystem;
    @Mock
    private HdfsFileSystem hdfsFileSystemThrowingExceptionOnGetStatusCall;
    @InjectMocks
    private Simple simple;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private HdfsConnection invalidAuthenticationConnection;
    @Mock
    private MuleFileSystem hdfsFileSystemThrowingAuthExceptionOnGetStatusCall;
    @Mock
    private HdfsConnection invalidConnectionRefusedConnection;
    @Mock
    private MuleFileSystem hdfsFileSystemThrowingConnRefusedExceptionOnGetStatusCall;

    @Before
    public void setUp() {
        mockValidSimpleSettings();
        mockValidAdvancedSettings();
        mockInvalidSimpleSettings();
        mockInvalidAdvancedSettings();
        mockConnectionBuilder();
        mockHdfsFileSystemProvider();
        mockHdfsFileSystemThrowingExceptionOnGetStatusCall();
        mockHdfsFileSystemThrowingAuthExceptionOnGetStatusCall();
        mockHdfsFileSystemThrowingConnRefusedExceptionOnGetStatusCall();
    }

    private void mockHdfsFileSystemThrowingConnRefusedExceptionOnGetStatusCall() {
        Mockito.when(hdfsFileSystemThrowingConnRefusedExceptionOnGetStatusCall.fileSystemStatus())
                .thenThrow(new ConnectionRefused("Connection refused.", null));
    }

    private void mockHdfsFileSystemThrowingAuthExceptionOnGetStatusCall() {
        Mockito.when(hdfsFileSystemThrowingAuthExceptionOnGetStatusCall.fileSystemStatus())
                .thenThrow(new AuthenticationFailed("Unable to authenticate connection.", null));
    }

    private void mockHdfsFileSystemThrowingExceptionOnGetStatusCall() {
        Mockito.when(hdfsFileSystemThrowingExceptionOnGetStatusCall.fileSystemStatus())
                .thenThrow(new RuntimeIO("Unable to establish connection.", null));
    }

    private void mockInvalidAdvancedSettings() {
        Mockito.when(invalidAdvancedSettings.getConfigurationResources())
                .thenReturn(INVALID_CONFIGURATION_RESOURCES);
        Mockito.when(invalidAdvancedSettings.getConfigurationEntries())
                .thenReturn(INVALID_CONFIGURATION_ENTRIES);
    }

    private void mockInvalidSimpleSettings() {
        Mockito.when(invalidSimpleSettings.getUsername())
                .thenReturn(INVALID_USERNAME);
        Mockito.when(invalidSimpleSettings.getNameNodeUri())
                .thenReturn(INVALID_NAME_NODE_URI);
    }

    private void mockValidAdvancedSettings() {
        Mockito.when(validAdvancedSettings.getConfigurationResources())
                .thenReturn(VALID_CONFIGURATION_RESOURCES);
        Mockito.when(validAdvancedSettings.getConfigurationEntries())
                .thenReturn(VALID_CONFIGURATION_ENTRIES);
    }

    private void mockValidSimpleSettings() {
        Mockito.when(validSimpleSettings.getUsername())
                .thenReturn(VALID_USERNAME);
        Mockito.when(validSimpleSettings.getNameNodeUri())
                .thenReturn(VALID_NAME_NODE_URI);
    }

    private void mockConnectionBuilder() {
        Mockito.when(hdfsConnectionBuilder.forSimpleAuth(Mockito.eq(VALID_NAME_NODE_URI), Mockito.eq(VALID_USERNAME),
                Mockito.refEq(VALID_CONFIGURATION_RESOURCES), Mockito.refEq(VALID_CONFIGURATION_ENTRIES)))
                .thenReturn(validConnection);
        Mockito.when(hdfsConnectionBuilder.forSimpleAuth(Mockito.eq(INVALID_NAME_NODE_URI), Mockito.eq(INVALID_USERNAME),
                Mockito.refEq(INVALID_CONFIGURATION_RESOURCES), Mockito.refEq(INVALID_CONFIGURATION_ENTRIES)))
                .thenReturn(wrongConnection);
    }

    private void mockHdfsFileSystemProvider() {
        Mockito.when(hdfsFileSystemProvider.fileSystem(validConnection))
                .thenReturn(hdfsFileSystem);
        Mockito.when(hdfsFileSystemProvider.fileSystem(wrongConnection))
                .thenReturn(hdfsFileSystemThrowingExceptionOnGetStatusCall);
        Mockito.when(hdfsFileSystemProvider.fileSystem(invalidAuthenticationConnection))
                .thenReturn(hdfsFileSystemThrowingAuthExceptionOnGetStatusCall);
        Mockito.when(hdfsFileSystemProvider.fileSystem(invalidConnectionRefusedConnection))
                .thenReturn(hdfsFileSystemThrowingConnRefusedExceptionOnGetStatusCall);
    }

    @Test
    public void thatConnectIsReturningAValidConnection() throws Exception {
        simple.setAdvancedSettings(validAdvancedSettings);
        simple.setSimpleSettings(validSimpleSettings);
        HdfsConnection hdfsConnection = simple.connect();
        assertThat(hdfsConnection, sameInstance(validConnection));
    }

    @Test
    public void thatConnectIsThrowingExceptionWhenInvalidConnection() throws Exception {
        simple.setAdvancedSettings(invalidAdvancedSettings);
        simple.setSimpleSettings(invalidSimpleSettings);
        expectedException.expect(ConnectionException.class);
        simple.connect();
    }

    @Test
    public void thatValidateIsReturningSuccessForValidConnection() {
        ConnectionValidationResult validationResult = simple.validate(validConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(true));
    }

    @Test
    public void thatValidateIsReturningUnknownFailureForInvalidConnection() {
        ConnectionValidationResult validationResult = simple.validate(wrongConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Unable to establish connection with server"));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.UNKNOWN));
    }

    @Test
    public void thatValidateIsReturningInvalidCredentialsFailureForInvalidAuthenticationConnection() {
        ConnectionValidationResult validationResult = simple.validate(invalidAuthenticationConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Failed to authenticate against server."));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.INCORRECT_CREDENTIALS));
    }

    @Test
    public void thatValidateIsReturningCannotReachFailureForInvalidConnectionRefusedConnection() {
        ConnectionValidationResult validationResult = simple.validate(invalidConnectionRefusedConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Server seems to be dead."));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.CANNOT_REACH));
    }

}
