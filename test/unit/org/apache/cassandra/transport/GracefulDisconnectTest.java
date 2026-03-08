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

package org.apache.cassandra.transport;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GracefulDisconnectTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();
    }

    @Before
    public void setupBefore()
    {
        reinitializeNetwork();
    }

    @Test
    public void testGracefulDisconnectReportedWhenEnabled() throws IOException
    {
        DatabaseDescriptor.setGracefulDisconnectEnabled(true);
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).protocolVersion(ProtocolVersion.V5);

        try (SimpleClient client = builder.build()) 
        {
            client.establishConnection();
            
            OptionsMessage message = new OptionsMessage();
            Response response = client.execute(message);

            if (!(response instanceof SupportedMessage)) Assertions.fail("Expected an SUPPORTED in response to a OPTIONS, got: " + response);
            
            SupportedMessage supportedMessage = (SupportedMessage) response;
            if (!supportedMessage.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT)) Assertions.fail("GRACEFUL_DISCONNECT event not received");
            if (!Boolean.parseBoolean(supportedMessage.supported.get(StartupMessage.GRACEFUL_DISCONNECT).get(0))) Assertions.fail("GRACEFUL_DISCONNECT value is false, expected true");
        }
    }
    

    @Test
    public void testGracefulDisconnectReportedAsFalseWhenDisabled() throws IOException
    {
        DatabaseDescriptor.setGracefulDisconnectEnabled(false);
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).protocolVersion(ProtocolVersion.V5);

        try (SimpleClient client = builder.build()) 
        {
            client.establishConnection();
            
            OptionsMessage message = new OptionsMessage();
            Response response = client.execute(message);

            if (!(response instanceof SupportedMessage)) Assertions.fail("Expected an SUPPORTED in response to a OPTIONS, got: " + response);
            
            SupportedMessage supportedMessage = (SupportedMessage) response;
            if (!supportedMessage.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT)) Assertions.fail("GRACEFUL_DISCONNECT event not received");
            if (Boolean.parseBoolean(supportedMessage.supported.get(StartupMessage.GRACEFUL_DISCONNECT).get(0))) Assertions.fail("GRACEFUL_DISCONNECT value is true, expected false");
        }
    }
}
