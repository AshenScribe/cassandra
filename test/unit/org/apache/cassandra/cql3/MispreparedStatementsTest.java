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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.guardrails.GuardrailViolatedException;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.junit.Assert.assertTrue;

public class MispreparedStatementsTest extends CQLTester
{
    private static final ClientState state = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));
    private static boolean originalValue;

    @BeforeClass
    public static void setupGlobalConfig()
    {
        CQLTester.requireAuthentication();
        originalValue = DatabaseDescriptor.getPreparedStatementsRequireParametersEnabled();
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(true);
    }

    @AfterClass
    public static void cleanUp()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(originalValue);
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
        createTable("CREATE TABLE %s (id int, description text, age int, name text, PRIMARY KEY (id, name, age))");
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(true);
    }

    @Test
    public void testSelectWithPartitionKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectWithClusteringKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name = 'v1' ALLOW FILTERING", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectWithFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1 AND name = 'v1'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectWithWhereNonPrimaryKeyColumn()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE description = 'foo' ALLOW FILTERING", currentTable()));
        assertNoWarnings();
    }


    @Test
    public void testSelectWithCompositeRestriction()
    {
        assertGuardrailViolated(String.format("SELECT sum(id) from %s WHERE (name, age) = ('a', 1) ALLOW FILTERING", currentTable()));
    }

    @Test
    public void testSelectWithFunction()
    {
        assertGuardrailViolated(String.format("SELECT sum(id) from %s where name = 'v1' ALLOW FILTERING", currentTable()));
    }

    @Test
    public void testSelectWithRangeRestriction()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1 AND name > 'a'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectInRestrictionOnPartitionKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id IN (1, 2, 3)", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectInRestrictionOnClusteringKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name IN ('a', 'b') ALLOW FILTERING", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectInRestrictionOnFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id IN (1, 2, 3) AND name in ('a', 'b')", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testDDLStatementsBypass()
    {
        assertGuardrailPassed("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY)");
        assertNoWarnings();
    }

    @Test
    public void testWhereLiteralWithLike()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name LIKE 'prefix%%' ALLOW FILTERING", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testInsertJsonGuardrail()
    {
        assertGuardrailViolated(String.format("INSERT INTO %s JSON '{\"id\": 1, \"name\": \"v1\"}'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testUpdateWithPartitionKey()
    {
        assertGuardrailViolated(String.format("UPDATE %s SET description = 'new' WHERE id = 1 AND name = 'name' AND age = 1", KEYSPACE + '.' + currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testUpdateWithIfCondition()
    {
        assertGuardrailViolated(String.format("UPDATE %s SET description = 'v2' WHERE id = 1 AND name = 'v1' AND age = 1 IF description = 'v0'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testDeleteWithFullPrimaryKey()
    {
        assertGuardrailViolated(String.format("DELETE FROM %s WHERE id = 1 AND name = 'v1'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testMultiTableBatchGuardrailAllLiteral()
    {
        String table1 = currentTable();
        String table2 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String query = String.format("BEGIN BATCH " +
                                     "UPDATE %s.%s SET description = 'v1' WHERE id = 1 AND name = 'n1' AND age = 1; " +
                                     "INSERT INTO %s.%s (id, val) VALUES (2, 'v2'); " +
                                     "APPLY BATCH",
                                     KEYSPACE, table1, KEYSPACE, table2);

        assertGuardrailViolated(query);
        assertNoWarnings();
    }

    @Test
    public void testSucceedWithOnlyBindMarkers()
    {
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = ? AND name = ?", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSucceedWithOneBindMarkerOneLiteral()
    {

        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = ? AND name = 'v1'", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSelectAllPasses()
    {
        assertGuardrailPassed(String.format("SELECT * FROM %s", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testLiteralInProjectionIsAllowed()
    {
        assertGuardrailPassed(String.format("SELECT id, (text)'const_val' FROM %s WHERE id = ?", currentTable()));
        assertNoWarnings();
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
    public void testGuardrailBypassesNonPrepareStatements()
    {
        String tableName = createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");

        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE id = 1 and name = 'v1' ALLOW FILTERING",
                                         KEYSPACE, tableName);

            QueryProcessor.process(query, ConsistencyLevel.ONE);
            assertNoWarnings();
        }
        catch (GuardrailViolatedException e)
        {
            Assert.fail("Non Prepare statement also checked for prepared constraints");
        }
    }


    @Test
    public void testGuardrailDoesNotApplyToNonPreparableStatements()
    {
        assertGuardrailPassed(String.format("ALTER TABLE %s add mime text", currentTable()));
        assertNoWarnings();
    }

    @Test
    public void testSystemKeyspaceBypassForRegularUser()
    {
        assertGuardrailPassed("SELECT * FROM system.local WHERE key = 'local'");
        assertNoWarnings();
    }

    @Test
    public void testGuardrailDisabledAllowsLiterals()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(false);
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        assertWarnings();
    }

    @Test
    public void testWarningIsIssuedWhenGuardrailIsAllowed()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(false);
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        assertWarnings();

    }

    @Test
    public void testGuardrailDisabledAllowsBatchLiterals()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(false);
        String tableName = currentTable();
        assertGuardrailPassed(String.format("BEGIN BATCH " +
                                            "INSERT INTO %s.%s (id, description, name, age) VALUES (1, 'v1', 'v1', 1); " +
                                            "UPDATE %s.%s SET description = 'v2' WHERE id = 2 AND name = 'v1' AND age = 1; " +
                                            "APPLY BATCH", KEYSPACE, tableName, KEYSPACE, tableName));
        assertWarnings();
    }

    @Test
    public void testBlockedStatementsDoNotEnterCache()
    {
        QueryProcessor.clearPreparedStatementsCache();
        int initialCacheSize = QueryProcessor.preparedStatementsCount();
        for (int i = 0; i < 10; i++)
        {
            String query = String.format("SELECT * FROM %s WHERE id = %d", currentTable(), i);
            try
            {
                QueryProcessor.instance.prepare(query, state);
            }
            catch (GuardrailViolatedException e)
            {
                // Expected: Guardrail throws exception
            }
        }
        Assert.assertEquals("Blocked misprepared statements should not be added to the cache", initialCacheSize, QueryProcessor.preparedStatementsCount());
    }

    private void assertGuardrailViolated(String query)
    {
        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected GuardrailViolatedException for query: " + query);
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue(e.getMessage().contains("Guardrail " + Guardrails.preparedStatementsRequireParametersEnabled.name + " violated"));
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    private void assertNoWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();
        if (warnings != null)
        {
            assertTrue("Expected performance tip warning was found",
                       warnings.stream().noneMatch(w -> w.contains(Guardrails.MISPREPARED_STATEMENT_WARN_MESSAGE)));
        }
    }

    private void assertWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();
        assertTrue("Expected performance tip warning was not found",
                   warnings.stream().anyMatch(w -> w.contains(Guardrails.MISPREPARED_STATEMENT_WARN_MESSAGE)));
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