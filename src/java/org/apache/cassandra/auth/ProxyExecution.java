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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.UnauthorizedException;

public final class ProxyExecution
{
    /**
     * Custom payload key for proxy execution.
     */
    public static final String PROXY_EXECUTE_KEY = "ProxyExecute";

    private ProxyExecution() {}

    /**
     * Validates that the proxy user is authorized to execute on behalf of the target user,
     * and returns the AuthenticatedUser object for the target role.
     *
     * @param proxyUser the authenticated user representing the proxy service (P1)
     * @param targetRoleName the name of the target user to execute as (U1)
     * @return the AuthenticatedUser for the target role
     * @throws UnauthorizedException if authorization or validation fails
     */
    public static AuthenticatedUser authorizeAndGetTargetUser(AuthenticatedUser proxyUser, String targetRoleName)
    {
        final IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        RoleResource targetRole = RoleResource.role(targetRoleName);
        if (!roleManager.isExistingRole(targetRole))
        {
            throw new UnauthorizedException(String.format("Role '%s' does not exist", targetRoleName));
        }
        if (!proxyUser.isSuper())
        {
            boolean isTargetSuper = roleManager.isSuper(targetRole);
            final IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();

            boolean hasPermission = false;
            if (authorizer.authorize(proxyUser, targetRole).contains(Permission.PROXY))
            {
                hasPermission = true;
            }
            else if (!isTargetSuper && authorizer.authorize(proxyUser, RoleResource.root()).contains(Permission.PROXY))
            {
                hasPermission = true;
            }

            if (!hasPermission)
            {
                throw new UnauthorizedException(String.format("Role '%s' is not authorized to proxy as '%s'",
                                                              proxyUser.getName(), targetRoleName));
            }
        }

        return new AuthenticatedUser(targetRoleName);
    }
}
