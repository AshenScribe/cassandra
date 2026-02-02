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

package org.apache.cassandra.cql3;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.GuardrailViolatedException;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.junit.Assert.assertTrue;

public class MispreparedStatementsTest extends CQLTester
{
    private static final ClientState state = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));
    private static final String TIP = "Using '?' placeholders (bind markers) is much more efficient";

    @BeforeClass
    public static void setupGlobalConfig()
    {
        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setAuthorizer(new CassandraAuthorizer());
        DatabaseDescriptor.setMispreparedStatementsEnabled(false);
    }

    @Before
    public void setUp()
    {
        AuthenticatedUser nonSuperUser = new AuthenticatedUser("regular-user")
        {
            @Override
            public boolean isSuper()
            {
                return false;
            }

            @Override
            public boolean isSystem()
            {
                return false;
            }

            @Override
            public boolean isAnonymous()
            {
                return false;
            }

            @Override
            public boolean canLogin()
            {
                return true;
            }
        };

        state.login(nonSuperUser);
        state.setKeyspace(KEYSPACE);
        ClientWarn.instance.captureWarnings();
        createTable("CREATE TABLE %s (id int, description text, name text, PRIMARY KEY (id, name))");
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setMispreparedStatementsEnabled(false);
    }

    @Test
    public void testSelectWithPartitionKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
    }

    @Test
    public void testSelectWithClusteringKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name = 'v1' ALLOW FILTERING", currentTable()));
    }

    @Test
    public void testSelectWithFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1 AND name = 'v1'", currentTable()));
    }

    @Test
    public void testSelectWithRangeRestriction()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1 AND name > 'a'", currentTable()));
    }

    @Test
    public void testSelectInRestrictionOnPartitionKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id IN (1, 2, 3)", currentTable()));
    }

    @Test
    public void testSelectInRestrictionOnClusteringKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name IN ('a', 'b') ALLOW FILTERING", currentTable()));
    }

    @Test
    public void testSelectInRestrictionOnFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id IN (1, 2, 3) AND name in ('a', 'b')", currentTable()));
    }

    @Test
    public void testInsertJsonGuardrail()
    {
        assertGuardrailViolated(String.format("INSERT INTO %s JSON '{\"id\": 1, \"name\": \"v1\"}'", currentTable()));
    }

    @Test
    public void testUpdateWithPartitionKey()
    {
        assertGuardrailViolated(String.format("UPDATE %s SET description = 'new' WHERE id = 1 AND name = 'name'", KEYSPACE + '.' + currentTable()));
    }

    @Test
    public void testUpdateWithIfCondition()
    {
        assertGuardrailViolated(String.format("UPDATE %s SET description = 'v2' WHERE id = 1 AND name = 'v1' IF description = 'v0'", currentTable()));
    }

    @Test
    public void testDeleteWithFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("DELETE FROM %s WHERE id = 1 AND name = 'v1'", currentTable()));
    }

    @Test
    public void testBatchGuardrail()
    {
        String tableName = currentTable();
        assertGuardrailViolated(String.format("BEGIN BATCH " +
                                              "INSERT INTO %s.%s (id, description, name) VALUES (1, 'v1', 'v1'); " +
                                              "UPDATE %s.%s SET description = 'v2' WHERE id = 2 AND name = 'v1'; " +
                                              "APPLY BATCH", KEYSPACE, tableName, KEYSPACE, tableName));
    }

    @Test
    public void testMultiTableBatchGuardrail()
    {
        String table1 = currentTable();
        String table2 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String query = String.format("BEGIN BATCH " +
                                     "UPDATE %s.%s SET description = 'v1' WHERE id = 1 AND name = 'n1'; " +
                                     "INSERT INTO %s.%s (id, val) VALUES (2, 'v2'); " +
                                     "APPLY BATCH",
                                     KEYSPACE, table1, KEYSPACE, table2);

        DatabaseDescriptor.setMispreparedStatementsEnabled(false);
        assertGuardrailViolated(query);

        DatabaseDescriptor.setMispreparedStatementsEnabled(true);
        assertGuardrailPassed(query);
    }

    @Test
    public void testProperlyPreparedWithBindMarkers()
    {
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = ? AND name = ?", currentTable()));
    }

    @Test
    public void testSelectAllPasses()
    {
        assertGuardrailPassed(String.format("SELECT * FROM %s", currentTable()));
    }

    @Test
    public void testLiteralInProjectionIsAllowed()
    {
        assertGuardrailPassed(String.format("SELECT id, (text)'const_val' FROM %s WHERE id = ?", currentTable()));
    }

    @Test
    public void testInternalBypass()
    {
        assertGuardrailPassed("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = 1", ClientState.forInternalCalls());
    }

    @Test
    public void testSuperUserBypass()
    {
        AuthenticatedUser superUser = new AuthenticatedUser("super-user")
        {
            @Override
            public boolean isSuper()
            {
                return true;
            }

            @Override
            public boolean isSystem()
            {
                return false;
            }

            @Override
            public boolean isAnonymous()
            {
                return false;
            }

            @Override
            public boolean canLogin()
            {
                return true;
            }
        };
        ClientState superUserState = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));
        superUserState.login(superUser);
        assertGuardrailPassed("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = 1", superUserState);
    }

    @Test
    public void testSystemKeyspaceBypassForRegularUser()
    {
        assertGuardrailPassed("SELECT * FROM system.local WHERE key = 'local'");
    }

    @Test
    public void testGuardrailDisabledAllowsLiterals()
    {
        DatabaseDescriptor.setMispreparedStatementsEnabled(true);
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
    }

    @Test
    public void testWarningIsIssuedWhenGuardrailIsAllowed()
    {
        DatabaseDescriptor.setMispreparedStatementsEnabled(true);
        ClientWarn.instance.captureWarnings();
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        List<String> warnings = ClientWarn.instance.getWarnings();
        Assert.assertNotNull(warnings);
        assertTrue(warnings.stream().anyMatch(w -> w.contains("Using '?' placeholders (bind markers) is much more efficient")));
    }

    @Test
    public void testGuardrailDisabledAllowsBatchLiterals()
    {
        DatabaseDescriptor.setMispreparedStatementsEnabled(true);
        String tableName = currentTable();
        assertGuardrailPassed(String.format("BEGIN BATCH " +
                                            "INSERT INTO %s.%s (id, description, name) VALUES (1, 'v1', 'v1'); " +
                                            "UPDATE %s.%s SET description = 'v2' WHERE id = 2 AND name = 'v1'; " +
                                            "APPLY BATCH", KEYSPACE, tableName, KEYSPACE, tableName));
    }

    private void assertGuardrailViolated(String query)
    {
        try
        {
            ClientWarn.instance.captureWarnings();
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected GuardrailViolatedException for query: " + query);
        }
        catch (Exception e)
        {
            assertTrue(e instanceof GuardrailViolatedException);
            List<String> warnings = ClientWarn.instance.getWarnings();
            if (warnings != null)
            {
                assertTrue("Performance tip should not be issued when blocking",
                           warnings.stream().noneMatch(w -> w.contains(Guardrails.MISPREPARED_STATEMENT_WARN_MESSAGE)));
            }
            assertTrue(e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    private void assertGuardrailPassed(String query)
    {
        assertGuardrailPassed(query, state);
    }

    private void assertGuardrailPassed(String query, ClientState clientState)
    {
        try
        {
            QueryProcessor.instance.prepare(query, clientState);
        }
        catch (Exception e)
        {
            Assert.fail("Expected guardrail to pass, but got: " + e.getMessage());
        }
    }
}