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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertTrue;

public class MispreparedStatementsTest extends CQLTester
{

    private static final ClientState state = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));

    @BeforeClass
    public static void setupGlobalConfig()
    {
        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setAuthorizer(new CassandraAuthorizer());
        DatabaseDescriptor.setUseMispreparedStatementsEnabled(false);
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
        createTable("CREATE TABLE %s (id int, description text, name text, PRIMARY KEY (id, name))");
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setUseMispreparedStatementsEnabled(false);
    }

    @Test
    public void testSelectWithPartitionKey()
    {
        String query = String.format("SELECT * FROM %s WHERE id = 1", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for mis-prepared SELECT statement");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testSelectWithClusteringKey()
    {
        String query = String.format("SELECT * FROM %s WHERE name = 'v1' ALLOW FILTERING", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for mis-prepared SELECT statement");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testInsertJsonGuardrail()
    {
        String query = String.format("INSERT INTO %s JSON '{\"id\": 1, \"name\": \"v1\"}'", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for INSERT JSON literal");
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testUpdateWithIfConditionLiteralShouldFail()
    {
        String query = String.format("UPDATE %s SET description = 'v2' WHERE id = 1 AND name = 'v1' IF description = 'v0'", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error because ZERO bind markers are present");
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }


    @Test
    public void testSelectWithFullPrimaryKey()
    {
        String query = String.format("SELECT * FROM %s WHERE id = 1 AND name = 'v1'", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for mis-prepared SELECT statement");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }

    }

    @Test
    public void testModificationGuardrail()
    {
        String fullName = KEYSPACE + '.' + currentTable();
        String query = String.format("UPDATE %s SET description = 'new_description' WHERE id = 1 AND name = 'name'", fullName);
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testDeleteWithFullPrimaryKey()
    {
        String query = String.format("DELETE FROM %s WHERE id = 1 AND name = 'v1'", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for DELETE with literals");
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testSelectWithRangeRestriction()
    {
        String query = String.format("SELECT * FROM %s WHERE id = 1 AND name > 'a'", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for range query with literals");
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testProperlyPreparedWithBindMarkers()
    {
        String query = String.format("SELECT * FROM %s WHERE id = ? AND name = ?", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
        }
        catch (Exception e)
        {
            Assert.fail("Guardrail should NOT trigger for statements using bind markers (?)");
        }
    }

    @Test
    public void testBatchGuardrail()
    {
        String tableName = currentTable();
        String batchWithLiterals = String.format("BEGIN BATCH " + "INSERT INTO %s.%s (id, description, name) VALUES (1, 'v1', 'v1'); " + "UPDATE %s.%s SET description = 'v2' WHERE id = 2 AND name = 'v1'; " + "APPLY BATCH", KEYSPACE, tableName, KEYSPACE, tableName);
        try
        {
            QueryProcessor.instance.prepare(batchWithLiterals, state);
            Assert.fail("Expected guardrail error for BATCH with literals");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testSelectInRestrictionOnPartitionKey() throws Throwable
    {
        String fullTableName = KEYSPACE + '.' + currentTable();
        String query = String.format("SELECT * FROM %s WHERE id IN (1, 2, 3)", fullTableName);
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for regular user with IN clause literals");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testSelectInRestrictionOnClusteringKey()
    {
        String fullTableName = KEYSPACE + '.' + currentTable();
        String query = String.format("SELECT * FROM %s WHERE name IN ('a', 'b') ALLOW FILTERING", fullTableName);
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for regular user with IN clause literals");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testSelectInRestrictionOnFullPrimaryKey()
    {
        String fullTableName = KEYSPACE + '.' + currentTable();
        String query = String.format("SELECT * FROM %s WHERE id IN (1, 2, 3) AND name in ('a', 'b')", fullTableName);
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected guardrail error for regular user with IN clause literals");
        }
        catch (Exception e)
        {
            assertTrue("Expected guardrail error, but got: " + e.getMessage(), e.getMessage().contains("Mis-prepared statements is not allowed"));
        }
    }

    @Test
    public void testInternalBypass()
    {
        ClientState internalState = ClientState.forInternalCalls();
        QueryProcessor.instance.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = 1", internalState);
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
        QueryProcessor.instance.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = 1", superUserState);
    }

    @Test
    public void testSelectAllPasses()
    {
        String query = String.format("SELECT * FROM %s", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
        }
        catch (Exception e)
        {
            Assert.fail("Did not expect guardrail error for SELECT * statement");
        }
    }

    @Test
    public void testGuardrailDisabledAllowsLiterals()
    {
        DatabaseDescriptor.setUseMispreparedStatementsEnabled(true);
        String query = String.format("SELECT * FROM %s WHERE id = 1", currentTable());
        try
        {
            QueryProcessor.instance.prepare(query, state);
        }
        catch (Exception e)
        {
            Assert.fail("Guardrail should NOT have triggered because it is disabled (set to true)");
        }
    }

    @Test
    public void testGuardrailDisabledAllowsBatchLiterals()
    {
        DatabaseDescriptor.setUseMispreparedStatementsEnabled(true);
        String tableName = currentTable();
        String batchWithLiterals = String.format("BEGIN BATCH " +
                                                 "INSERT INTO %s.%s (id, description, name) VALUES (1, 'v1', 'v1'); " +
                                                 "UPDATE %s.%s SET description = 'v2' WHERE id = 2 AND name = 'v1'; " +
                                                 "APPLY BATCH", KEYSPACE, tableName, KEYSPACE, tableName);
        try
        {
            QueryProcessor.instance.prepare(batchWithLiterals, state);
        }
        catch (Exception e)
        {
            Assert.fail("Batch with literals should be allowed when guardrail is disabled");
        }
    }
}

