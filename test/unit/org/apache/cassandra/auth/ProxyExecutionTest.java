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

package org.apache.cassandra.auth;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ProxyExecutionTest extends CQLTester
{
    private static final InetSocketAddress CLIENT_ADDR = new InetSocketAddress("127.0.0.1", 9042);

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
    }

    @Before
    public void setupTest() throws Throwable
    {
        executeAsSuperuser("CREATE KEYSPACE IF NOT EXISTS proxy_test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeAsSuperuser("CREATE TABLE IF NOT EXISTS proxy_test_ks.secrets (id int PRIMARY KEY, val text)");

        executeAsSuperuser("CREATE ROLE IF NOT EXISTS proxy_service WITH PASSWORD = 'password' AND LOGIN = true");
        executeAsSuperuser("CREATE ROLE IF NOT EXISTS target_user WITH PASSWORD = 'password' AND LOGIN = true");
        executeAsSuperuser("CREATE ROLE IF NOT EXISTS target_superuser WITH PASSWORD = 'password' AND LOGIN = true AND SUPERUSER = true");

        executeAsSuperuser("GRANT SELECT ON TABLE proxy_test_ks.secrets TO target_user");

        AuthCacheService.instance.invalidateCaches();
    }

    private void executeAsSuperuser(String query)
    {
        try
        {
            ClientState superuserState = ClientState.forExternalCalls(CLIENT_ADDR);
            superuserState.login(new AuthenticatedUser("cassandra"));
            QueryState queryState = new QueryState(superuserState);

            CQLStatement statement = QueryProcessor.parseStatement(query, superuserState);
            QueryProcessor.instance.process(statement,
                                            queryState,
                                            QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Dispatcher.RequestTime.forImmediateExecution());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to authenticate and execute setup query: " + query, e);
        }
    }

    private Message.Response executeAs(QueryState state, String query)
    {
        CQLStatement statement = QueryProcessor.parseStatement(query, state.getClientState());
        return QueryProcessor.instance.process(statement,
                                               state,
                                               QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Collections.emptyList()),
                                               Collections.emptyMap(),
                                               Dispatcher.RequestTime.forImmediateExecution());
    }

    @Test
    public void testEndToEndProxyExecution() throws Throwable
    {
        ClientState clientState = ClientState.forExternalCalls(CLIENT_ADDR);
        clientState.login(new AuthenticatedUser("proxy_service"));

        Map<String, ByteBuffer> payload = Map.of(ProxyExecution.PROXY_EXECUTE_KEY, ByteBufferUtil.bytes("target_user"));

        Assertions.assertThatThrownBy(() -> {
            QueryState.forRequest(clientState, payload);
        }).isInstanceOf(UnauthorizedException.class);

        QueryState baseState = QueryState.forRequest(clientState, null);
        Assertions.assertThatThrownBy(() -> {
            executeAs(baseState, "SELECT * FROM proxy_test_ks.secrets");
        }).isInstanceOf(UnauthorizedException.class);

        executeAsSuperuser("GRANT PROXY ON ROLE target_user TO proxy_service");
        AuthCacheService.instance.invalidateCaches();

        QueryState proxiedState = QueryState.forRequest(clientState, payload);
        Assertions.assertThat(proxiedState.getClientState().getUser().getName()).isEqualTo("target_user");
        Assertions.assertThat(proxiedState.getClientState().getAuthenticatedUser().getName()).isEqualTo("proxy_service");

        Message.Response proxiedResponse = executeAs(proxiedState, "SELECT * FROM proxy_test_ks.secrets");
        Assertions.assertThat(proxiedResponse).isInstanceOf(ResultMessage.class);
    }

    @Test
    public void testWildcardProxySuperuserRestrictions() throws Throwable
    {
        ClientState clientState = ClientState.forExternalCalls(CLIENT_ADDR);
        clientState.login(new AuthenticatedUser("proxy_service"));

        executeAsSuperuser("GRANT PROXY ON ALL ROLES TO proxy_service");
        AuthCacheService.instance.invalidateCaches();

        Map<String, ByteBuffer> targetUserPayload = Map.of(ProxyExecution.PROXY_EXECUTE_KEY, ByteBufferUtil.bytes("target_user"));
        QueryState targetUserState = QueryState.forRequest(clientState, targetUserPayload);
        Assertions.assertThat(targetUserState.getClientState().getUser().getName()).isEqualTo("target_user");

        Map<String, ByteBuffer> superuserPayload = Map.of(ProxyExecution.PROXY_EXECUTE_KEY, ByteBufferUtil.bytes("target_superuser"));
        Assertions.assertThatThrownBy(() -> {
            QueryState.forRequest(clientState, superuserPayload);
        }).isInstanceOf(UnauthorizedException.class);

        executeAsSuperuser("GRANT PROXY ON ROLE target_superuser TO proxy_service");
        AuthCacheService.instance.invalidateCaches();

        QueryState superuserState = QueryState.forRequest(clientState, superuserPayload);
        Assertions.assertThat(superuserState.getClientState().getAuthenticatedUser().getName()).isEqualTo("proxy_service");
        Assertions.assertThat(superuserState.getClientState().getUser().getName()).isEqualTo("target_superuser");
    }
}
