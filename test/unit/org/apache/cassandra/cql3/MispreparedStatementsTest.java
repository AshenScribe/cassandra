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

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.GuardrailViolatedException;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

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
    public void testViolationOnLiterals()
    {
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name = 'v1' ALLOW FILTERING", currentTable()));

        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id = 1 AND name > 'a'", currentTable()));
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE id IN (1, 2, 3)", currentTable()));
        assertGuardrailViolated(String.format("SELECT sum(id) from %s WHERE (name, age) = ('a', 1) ALLOW FILTERING", currentTable()));
        assertGuardrailViolated(String.format("SELECT * FROM %s WHERE name LIKE 'prefix%%' ALLOW FILTERING", currentTable()));

        assertGuardrailViolated(String.format("UPDATE %s SET description = 'new' WHERE id = 1 AND name = 'n' AND age = 1", currentTable()));
        assertGuardrailViolated(String.format("DELETE FROM %s WHERE id = 1 AND name = 'v1'", currentTable()));

        String batch = String.format("BEGIN BATCH UPDATE %s SET description = 'v1' WHERE id = 1 AND name = 'n1' AND age = 1; APPLY BATCH", currentTable());
        assertGuardrailViolated(batch);
    }


    @Test
    public void testValidPreparedStatements()
    {
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = ? AND name = ?", currentTable()));

        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = ? AND name = 'v1'", currentTable()));

        assertGuardrailPassed(String.format("SELECT * FROM %s", currentTable()));

        assertGuardrailPassed(String.format("SELECT id, (text)'const_val' FROM %s WHERE id = ?", currentTable()));
    }

    @Test
    public void testNonPreparableAndSystemBypass()
    {
        assertGuardrailPassed("CREATE TABLE IF NOT EXISTS test_bypass (id INT PRIMARY KEY)");
        assertGuardrailPassed(String.format("ALTER TABLE %s ADD mime text", currentTable()));
        assertGuardrailPassed(String.format("INSERT INTO %s JSON '{\"id\": 1}'", currentTable()));
        assertGuardrailPassed("SELECT * FROM system.local WHERE key = 'local'");
    }


    @Test
    public void testAuthorizedBypasses()
    {
        assertGuardrailPassed("SELECT * FROM " + KEYSPACE + "." + currentTable() + " WHERE id = 1", ClientState.forInternalCalls());

        ClientState superState = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));
        superState.login(createMockUser("super-user", true));
        assertGuardrailPassed("SELECT * FROM " + KEYSPACE + "." + currentTable() + " WHERE id = 1", superState);
    }


    @Test
    public void testWarningBehaviors()
    {
        DatabaseDescriptor.setPreparedStatementsRequireParametersEnabled(false);
        assertGuardrailPassed(String.format("SELECT * FROM %s WHERE id = 1", currentTable()));
        assertWarnings();

        ClientWarn.instance.resetWarnings();
        QueryProcessor.instance.prepare(String.format("SELECT * FROM %s WHERE id = 1", currentTable()), state);
        assertNoWarnings(); // Second time should be silent due to cache/id match
    }

    @Test
    public void testCacheIntegrity()
    {
        QueryProcessor.clearPreparedStatementsCache();
        int initialCount = QueryProcessor.preparedStatementsCount();

        for (int i = 0; i < 5; i++)
        {
            String query = String.format("SELECT * FROM %s WHERE id = %d", currentTable(), i);
            Assertions.assertThatThrownBy(() -> QueryProcessor.instance.prepare(query, state)).isInstanceOf(GuardrailViolatedException.class);
        }

        Assert.assertEquals("Violated statements must not be cached", initialCount, QueryProcessor.preparedStatementsCount());
    }

    private void assertGuardrailViolated(String query)
    {
        Assertions.assertThatThrownBy(() -> {
                      QueryProcessor.instance.prepare(query, state);
                  }).isInstanceOf(GuardrailViolatedException.class)
                  .hasMessageContaining("Guardrail " + Guardrails.preparedStatementsRequireParameters.name + " violated");
    }

    private void assertNoWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();
        if (warnings != null)
        {
            Assert.assertTrue("Unexpected performance tip warning was found",
                              warnings.stream().noneMatch(w -> w.contains(Guardrails.MISPREPARED_STATEMENT_WARN_MESSAGE)));
        }
    }

    private void assertWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        Assert.assertNotNull("Expected performance tip warning was not found (warnings list was null)", warnings);

        Assert.assertTrue("Expected performance tip warning was not found in: " + warnings,
                          warnings.stream().anyMatch(w -> w.contains(Guardrails.MISPREPARED_STATEMENT_WARN_MESSAGE)));
    }

    private AuthenticatedUser createMockUser(String name, boolean isSuper)
    {
        return new AuthenticatedUser(name)
        {
            public boolean isSuper()
            {
                return isSuper;
            }

            public boolean isSystem()
            {
                return false;
            }

            public boolean isAnonymous()
            {
                return false;
            }

            public boolean canLogin()
            {
                return true;
            }
        };
    }

    private void assertGuardrailPassed(String query)
    {
        assertGuardrailPassed(query, state);
    }

    private void assertGuardrailPassed(String query, ClientState clientState)
    {
        QueryProcessor.instance.prepare(query, clientState);
    }
}