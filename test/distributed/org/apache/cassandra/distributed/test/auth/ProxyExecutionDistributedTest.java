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

package org.apache.cassandra.distributed.test.auth;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.ProxyExecution;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProxyExecutionDistributedTest extends TestBaseImpl
{
    @BeforeClass
    public static void setUp()
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
    }

    private static void executeAsSuperuser(IInvokableInstance node, String query)
    {
        node.runOnInstance(() -> {
            try
            {
                org.apache.cassandra.service.ClientState superuserState = org.apache.cassandra.service.ClientState.forExternalCalls(
                new java.net.InetSocketAddress("127.0.0.1", 9042)
                );
                superuserState.login(new org.apache.cassandra.auth.AuthenticatedUser("cassandra"));
                org.apache.cassandra.service.QueryState queryState = new org.apache.cassandra.service.QueryState(superuserState);

                org.apache.cassandra.cql3.CQLStatement statement = org.apache.cassandra.cql3.QueryProcessor.parseStatement(query, superuserState);
                org.apache.cassandra.cql3.QueryProcessor.instance.process(
                statement,
                queryState,
                org.apache.cassandra.cql3.QueryOptions.forInternalCalls(org.apache.cassandra.db.ConsistencyLevel.ONE, java.util.Collections.emptyList()),
                java.util.Collections.emptyMap(),
                org.apache.cassandra.transport.Dispatcher.RequestTime.forImmediateExecution()
                );
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to execute query as superuser on node: " + query, e);
            }
        });
    }

    @Test
    public void testProxyExecutionOverNativeProtocol() throws Throwable
    {
        try (org.apache.cassandra.distributed.Cluster dtestCluster = org.apache.cassandra.distributed.Cluster.build(1)
                                                                                                             .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                                                                                                                                         .set("authenticator", "PasswordAuthenticator")
                                                                                                                                         .set("authorizer", "CassandraAuthorizer")
                                                                                                                                         .set("role_manager", "CassandraRoleManager"))
                                                                                                             .start())
        {
            executeAsSuperuser(dtestCluster.get(1), "CREATE KEYSPACE proxy_test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            executeAsSuperuser(dtestCluster.get(1), "CREATE TABLE proxy_test_ks.secrets (id int PRIMARY KEY, val text)");
            executeAsSuperuser(dtestCluster.get(1), "INSERT INTO proxy_test_ks.secrets (id, val) VALUES (1, 'very_secret_data')");

            executeAsSuperuser(dtestCluster.get(1), "CREATE ROLE proxy_service WITH PASSWORD = 'password' AND LOGIN = true");
            executeAsSuperuser(dtestCluster.get(1), "CREATE ROLE target_user WITH PASSWORD = 'password' AND LOGIN = true");

            executeAsSuperuser(dtestCluster.get(1), "GRANT SELECT ON TABLE proxy_test_ks.secrets TO target_user");
            executeAsSuperuser(dtestCluster.get(1), "GRANT PROXY ON ROLE target_user TO proxy_service");

            dtestCluster.get(1).runOnInstance(AuthCacheService.instance::invalidateCaches);

            InetSocketAddress contactPoint = dtestCluster.get(1).config().broadcastAddress();
            int nativePort = Integer.parseInt(dtestCluster.get(1).config().get("native_transport_port").toString());

            try (com.datastax.driver.core.Cluster driverCluster = com.datastax.driver.core.Cluster.builder()
                                                                                                  .addContactPoints(contactPoint.getAddress())
                                                                                                  .withPort(nativePort)
                                                                                                  .withCredentials("proxy_service", "password")
                                                                                                  .build();
                 Session session = driverCluster.connect())
            {
                Statement baseStatement = new SimpleStatement("SELECT * FROM proxy_test_ks.secrets WHERE id = 1");
                try
                {
                    session.execute(baseStatement);
                    fail("Query should have failed because proxy_service does not have SELECT permission");
                }
                catch (UnauthorizedException e)
                {
                    assertTrue(e.getMessage().contains("User proxy_service has no SELECT permission"));
                }

                Statement proxiedStatement = new SimpleStatement("SELECT * FROM proxy_test_ks.secrets WHERE id = 1");
                Map<String, ByteBuffer> payload = Map.of(ProxyExecution.PROXY_EXECUTE_KEY, ByteBufferUtil.bytes("target_user"));
                proxiedStatement.setOutgoingPayload(payload);

                com.datastax.driver.core.ResultSet rs = session.execute(proxiedStatement);
                List<com.datastax.driver.core.Row> rows = rs.all();

                assertEquals(1, rows.size());
                assertEquals("very_secret_data", rows.get(0).getString("val"));
            }
        }
    }
}
