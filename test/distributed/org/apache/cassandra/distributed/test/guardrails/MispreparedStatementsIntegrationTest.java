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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.assertj.core.api.ListAssert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MispreparedStatementsIntegrationTest extends GuardrailTester
{

    private static EmbeddedCassandraService cassandra;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Cluster cluster;
    private static Session driverSession;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build(1)
                              .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK)
                                                .set("authenticator", "PasswordAuthenticator")
                                                .set("prepared_statements_require_parameters_enabled", true))
                              .start());
        driverCluster = buildDriverCluster(cluster);
        driverSession = driverCluster.connect();
    }


    @Before
    public void beforeTest()
    {
        super.beforeTest();
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
        driverSession = driverCluster.connect();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    public static void tearDown()
    {
        if (cassandra != null)
        {
            cassandra.stop();
        }
    }

    @Test
    public void testInvalidConstantSelectStatements()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, data1 text, data2 text, PRIMARY KEY((pk1, pk2), ck1, ck2))");

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk2 = 1"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck1 = 1 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck2 = 1 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck1 = 1 AND ck2 = 1 ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE data1 = 'val' ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE data1 = 'val' AND data2 = 'val' ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND ck1 = 1 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk2 = 1 AND ck1 = 1 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND ck2 = 1 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND data1 = 'val' ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND pk2 = 1 AND ck2 = 1 ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND pk2 = 1 AND data1 = 'val'"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 IN (1, 2)"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk2 IN (1, 2)"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck1 IN (1, 2) ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck1 > 10 ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE pk1 = 1 AND ck1 > 10 ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE data1 LIKE 'prefix%%' ALLOW FILTERING"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("SELECT * FROM %s WHERE ck1 LIKE 'abc%%' ALLOW FILTERING"));

        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("UPDATE %s SET data1 = 'new' WHERE pk1 = 1 AND ck1 = 1 AND ck2 = 1"));
        Assertions.assertThrows(InvalidQueryException.class, () -> prepare("UPDATE %s SET data1 = 'new' WHERE pk1 = 1 AND pk2 = 1 AND ck2 = 1"));
        assertNotWarns();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Override
    protected Session getSession()
    {
        return driverSession;
    }

    private void prepare(String query, Object... args)
    {
        SimpleStatement stmt = new SimpleStatement(format(query), args);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        driverSession.prepare(stmt);
    }

    private void createTable(String cql)
    {
        schemaChange(cql);
        for (IInvokableInstance instance : cluster)
        {
            instance.runOnInstance(() -> {
                for (ColumnFamilyStore cs : Keyspace.open(KEYSPACE).getColumnFamilyStores())
                    cs.disableAutoCompaction();
            });
        }
    }

    protected void assertNotWarns()
    {
        getCluster().stream().forEach(this::assertNotWarns);
    }

    protected void assertNotWarns(IInstance node)
    {
        long mark = node.logs().mark();
        try
        {
            assertTrue(node.logs().grep(mark, "^ERROR", "^WARN").getResult().isEmpty());
        }
        catch (InvalidRequestException e)
        {
            fail("Expected not to fail, but Fails with error message: " + e.getMessage());
        }
    }


    protected void assertWarns(String... msgs)
    {
        getCluster().stream().forEach(node -> assertWarns(node, msgs));
    }

    protected void assertWarns(IInstance node, String... msgs)
    {
        long mark = node.logs().mark();
        assertTrue(node.logs().grep(mark, "^ERROR").getResult().isEmpty());
        List<String> warnings = node.logs().grep(mark, "^WARN").getResult();
        ListAssert<String> assertion = org.assertj.core.api.Assertions.assertThat(warnings).isNotEmpty().hasSize(msgs.length);
        for (String msg : msgs)
            assertion.anyMatch(m -> m.contains(msg));
    }
}
