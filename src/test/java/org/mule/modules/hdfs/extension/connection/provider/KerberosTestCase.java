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
import org.mule.modules.hdfs.extension.dto.connection.AdvancedSettings;
import org.mule.modules.hdfs.extension.dto.connection.KerberosSettings;
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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author MuleSoft, Inc.
 */
@RunWith(MockitoJUnitRunner.class)
public class KerberosTestCase {

    private static final String VALID_NAME_NODE_URI = "hdfs://localhost:9000";
    private static final String VALID_KEYTAB_PATH = "hdfs.keytab";
    private static final String VALID_PRINCIPAL = "principal/localhost@LOCALHOST";
    private static final List<String> VALID_CONFIGURATION_RESOURCES = new ArrayList<>();
    private static final Map<String, String> VALID_CONFIGURATION_ENTRIES = new HashMap<>();
    private static final String INVALID_NAME_NODE_URI = "hdfs://invalid:9000";
    private static final String INVALID_KEYTAB_PATH = "invalidhdfs.keytab";
    private static final String INVALID_PRINCIPAL = "invalid/localhost@LOCALHOST";
    private static final List<String> INVALID_CONFIGURATION_RESOURCES = new ArrayList<>();
    private static final Map<String, String> INVALID_CONFIGURATION_ENTRIES = new HashMap<>();
    @Mock
    private KerberosSettings validKerberosSettings;
    @Mock
    private AdvancedSettings validAdvancedSettings;
    @Mock
    private KerberosSettings invalidKerberosSettings;
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
    private Kerberos kerberos;
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
        mockValidKerberosSettings();
        mockValidAdvancedSettings();
        mockInvalidKerberosSettings();
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

    private void mockInvalidKerberosSettings() {
        Mockito.when(invalidKerberosSettings.getPrincipal())
                .thenReturn(INVALID_PRINCIPAL);
        Mockito.when(invalidKerberosSettings.getNameNodeUri())
                .thenReturn(INVALID_NAME_NODE_URI);
        Mockito.when(invalidKerberosSettings.getKeytabPath())
                .thenReturn(INVALID_KEYTAB_PATH);
    }

    private void mockValidAdvancedSettings() {
        Mockito.when(validAdvancedSettings.getConfigurationResources())
                .thenReturn(VALID_CONFIGURATION_RESOURCES);
        Mockito.when(validAdvancedSettings.getConfigurationEntries())
                .thenReturn(VALID_CONFIGURATION_ENTRIES);
    }

    private void mockValidKerberosSettings() {
        Mockito.when(validKerberosSettings.getPrincipal())
                .thenReturn(VALID_PRINCIPAL);
        Mockito.when(validKerberosSettings.getNameNodeUri())
                .thenReturn(VALID_NAME_NODE_URI);
        Mockito.when(validKerberosSettings.getKeytabPath())
                .thenReturn(VALID_KEYTAB_PATH);
    }

    private void mockConnectionBuilder() {
        Mockito.when(hdfsConnectionBuilder.forKerberosAuth(Mockito.eq(VALID_NAME_NODE_URI), Mockito.eq(VALID_PRINCIPAL),
                Mockito.eq(VALID_KEYTAB_PATH), Mockito.refEq(VALID_CONFIGURATION_RESOURCES), Mockito.refEq(VALID_CONFIGURATION_ENTRIES)))
                .thenReturn(validConnection);
        Mockito.when(hdfsConnectionBuilder.forKerberosAuth(Mockito.eq(INVALID_NAME_NODE_URI), Mockito.eq(INVALID_PRINCIPAL),
                Mockito.eq(INVALID_KEYTAB_PATH), Mockito.refEq(INVALID_CONFIGURATION_RESOURCES), Mockito.refEq(INVALID_CONFIGURATION_ENTRIES)))
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
        kerberos.setAdvancedSettings(validAdvancedSettings);
        kerberos.setKerberosSettings(validKerberosSettings);
        HdfsConnection hdfsConnection = kerberos.connect();
        assertThat(hdfsConnection, sameInstance(validConnection));
    }

    @Test
    public void thatConnectIsThrowingExceptionWhenInvalidConnection() throws Exception {
        kerberos.setAdvancedSettings(invalidAdvancedSettings);
        kerberos.setKerberosSettings(invalidKerberosSettings);
        expectedException.expect(ConnectionException.class);
        kerberos.connect();
    }

    @Test
    public void thatValidateIsReturningSuccessForValidConnection() {
        ConnectionValidationResult validationResult = kerberos.validate(validConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(true));
    }

    @Test
    public void thatValidateIsReturningUnknownFailureForInvalidConnection() {
        ConnectionValidationResult validationResult = kerberos.validate(wrongConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Unable to establish connection with server"));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.UNKNOWN));
    }

    @Test
    public void thatValidateIsReturningInvalidCredentialsFailureForInvalidAuthenticationConnection() {
        ConnectionValidationResult validationResult = kerberos.validate(invalidAuthenticationConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Failed to authenticate against server."));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.INCORRECT_CREDENTIALS));
    }

    @Test
    public void thatValidateIsReturningCannotReachFailureForInvalidConnectionRefusedConnection() {
        ConnectionValidationResult validationResult = kerberos.validate(invalidConnectionRefusedConnection);
        assertThat(validationResult, notNullValue());
        assertThat(validationResult.isValid(), is(false));
        assertThat(validationResult.getMessage(), equalTo("Server seems to be dead."));
        assertThat(validationResult.getCode(), equalTo(ConnectionExceptionCode.CANNOT_REACH));
    }
}
