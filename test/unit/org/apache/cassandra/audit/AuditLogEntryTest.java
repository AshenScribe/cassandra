/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.audit;

import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.auth.MutualTlsAuthenticator.METADATA_IDENTITY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AuditLogEntry}
 */
public class AuditLogEntryTest
{
    AuditLogEntry entry;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 9999);

        ImmutableMap<String, Object> userMetadata = ImmutableMap.of(METADATA_IDENTITY_KEY, "cassandra_user_identity");
        AuthenticatedUser mockAuthenticatedUser = mock(AuthenticatedUser.class);
        when(mockAuthenticatedUser.getName()).thenReturn("cassandra_user");
        when(mockAuthenticatedUser.getMetadata()).thenReturn(userMetadata);

        ClientState mockClientState = mock(ClientState.class);
        when(mockClientState.getRemoteAddress()).thenReturn(remoteAddress);
        when(mockClientState.getUser()).thenReturn(mockAuthenticatedUser);

        when(mockClientState.getAuthenticatedUser()).thenReturn(mockAuthenticatedUser);
        when(mockClientState.getKeyspace()).thenReturn("test_keyspace");

        QueryState mockQueryState = mock(QueryState.class);
        when(mockQueryState.getClientState()).thenReturn(mockClientState);

        entry = new AuditLogEntry.Builder(mockQueryState)
                .setOperation("LOGIN SUCCESSFUL")
                .setType(AuditLogEntryType.LOGIN_SUCCESS)
                .build();
    }

    @Test
    public void testDefaultGetLogString()
    {
        assertThat(entry.getLogString()).matches("user:cassandra_user\\|host:/127.0.0.1:7012\\|" +
                                                 "source:/127.0.0.1\\|port:9999\\|timestamp:\\d+\\|" +
                                                 "type:LOGIN_SUCCESS\\|category:AUTH\\|" +
                                                 "operation:LOGIN SUCCESSFUL\\|identity:cassandra_user_identity");
    }

    @Test
    public void testGetLogStringWithCustomSeparators()
    {
        assertThat(entry.getLogString("=", " ")).matches("user=cassandra_user host=/127.0.0.1:7012 " +
                                                         "source=/127.0.0.1 port=9999 timestamp=\\d+ " +
                                                         "type=LOGIN_SUCCESS category=AUTH " +
                                                         "operation=LOGIN SUCCESSFUL identity=cassandra_user_identity");
    }

    @Test
    public void testProxyExecutionGetLogString()
    {
        InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 9999);

        AuthenticatedUser mockProxyUser = mock(AuthenticatedUser.class);
        when(mockProxyUser.getName()).thenReturn("proxy_service");

        AuthenticatedUser mockTargetUser = mock(AuthenticatedUser.class);
        when(mockTargetUser.getName()).thenReturn("target_user");

        ClientState mockClientState = mock(ClientState.class);
        when(mockClientState.getRemoteAddress()).thenReturn(remoteAddress);
        when(mockClientState.getAuthenticatedUser()).thenReturn(mockProxyUser); // P1
        when(mockClientState.getUser()).thenReturn(mockTargetUser);             // U1

        QueryState mockQueryState = mock(QueryState.class);
        when(mockQueryState.getClientState()).thenReturn(mockClientState);

        AuditLogEntry proxyEntry = new AuditLogEntry.Builder(mockQueryState)
                                   .setOperation("SELECT * FROM foo")
                                   .setType(AuditLogEntryType.SELECT)
                                   .build();

        // Verify that BOTH the caller (proxy_service) and effective user (target_user) are logged
        assertThat(proxyEntry.getLogString()).contains("user:proxy_service|executed_as:target_user");
    }
}
