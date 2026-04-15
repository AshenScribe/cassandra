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

package org.apache.cassandra.db.guardrails;

import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.TriPredicate;

public class PreparedStatementParameterRequirementGuardrail extends Guardrail
{
    private final TriPredicate<ClientState, CQLStatement, StatementRestrictions> warnPredicate;
    private final TriPredicate<ClientState, CQLStatement, StatementRestrictions> failurePredicate;

    PreparedStatementParameterRequirementGuardrail()
    {
        super("prepared_statements_require_parameters", null);
        this.warnPredicate = new WarnEvaluationFunction();
        this.failurePredicate = new FailEvaluationFunction();
    }

    public void guard(CQLStatement statement, StatementRestrictions restrictions, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        if (failurePredicate.test(state, statement, restrictions))
            fail("warn", state);
        else if (warnPredicate.test(state, statement, restrictions))
            warn("fail");
    }

    public static final class FailEvaluationFunction extends EvaluationFunction
    {
        public FailEvaluationFunction()
        {
            super(state -> Guardrails.CONFIG_PROVIDER.getOrCreate(state).getPreparedStatementsRequireParametersFail());
        }
    }

    public static final class WarnEvaluationFunction extends EvaluationFunction
    {
        public WarnEvaluationFunction()
        {
            super(state -> Guardrails.CONFIG_PROVIDER.getOrCreate(state).getPreparedStatementsRequireParametersWarn());
        }
    }

    public static abstract class EvaluationFunction implements TriPredicate<ClientState, CQLStatement, StatementRestrictions>
    {
        private final Predicate<ClientState> enablementPredicate;

        public EvaluationFunction(Predicate<ClientState> enablementPredicate)
        {
            this.enablementPredicate = enablementPredicate;
        }

        @Override
        public boolean test(ClientState state, CQLStatement cqlStatement, StatementRestrictions restrictions)
        {
            // fail as fast as possible
            if (restrictions == null || !cqlStatement.eligibleAsPreparedStatement())
                return false;

            if (!enablementPredicate.test(state))
                return false;

            boolean hasRestrictions = restrictions.hasPartitionKeyRestrictions()
                                      || restrictions.hasClusteringColumnsRestrictions()
                                      || restrictions.hasNonPrimaryKeyRestrictions();

            if (!hasRestrictions)
                return false;

            return cqlStatement.getBindVariables().isEmpty();
        }
    }
}

