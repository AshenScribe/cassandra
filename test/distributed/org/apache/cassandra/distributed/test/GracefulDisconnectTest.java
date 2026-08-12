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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

public class GracefulDisconnectTest
{
    @BeforeClass
    public static void setUp() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public Cluster buildCluster(int nodeCount, boolean gracefulDisconnectEnabled) throws IOException
    {
        return Cluster
               .build(nodeCount)
               .withConfig(config ->
                           config
                           .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                           .set("graceful_disconnect_enabled", gracefulDisconnectEnabled))
               .start();
    }

    @Test
    public void testGracefulDisconnectAdvertisedWhenEnabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                Message.Response response = client.execute(new OptionsMessage());
                SupportedMessage supported = (SupportedMessage) response;

                Assertions.assertThat(supported.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT))
                          .as("GRACEFUL_DISCONNECT should be advertised in SUPPORTED when enabled")
                          .isTrue();
            }
        }
    }

    @Test
    public void testGracefulDisconnectDoesNotAdvertisedWhenNotEnabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, false))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                SupportedMessage supported = (SupportedMessage) client.execute(new OptionsMessage());

                Assertions.assertThat(supported.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT))
                          .as("GRACEFUL_DISCONNECT should NOT be advertised in SUPPORTED when disabled")
                          .isFalse();
            }
        }
    }

    @Test
    public void testSubscriptionViaREGISTER() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                Message.Response response = client.execute(
                new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                Assertions.assertThat(response).isInstanceOf(ReadyMessage.class);

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertThat(subscribedCount)
                          .as("One channel should be subscribed to GRACEFUL_DISCONNECT")
                          .isEqualTo(1);
            }
        }
    }

    @Test
    public void testRegisterGracefulDisconnectRejectedOnV4() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).protocolVersion(ProtocolVersion.V4).build())
            {
                client.connect(false);

                Assertions.assertThatThrownBy(() -> client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT))))
                          .hasCauseInstanceOf(org.apache.cassandra.transport.ProtocolException.class);

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertThat(subscribedCount)
                          .as("V4 client should not be able to subscribe")
                          .isEqualTo(0);
            }
        }
    }

    @Test
    public void testNonSubscribedClientDoesNotReceiveEvent() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V4)
                                                   .build())
            {
                client.connect(false);
                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());
                Assertions.assertThat(subscribedCount).isEqualTo(0);
            }
        }
    }

    @Test
    public void testServerStopsAcceptingNewConnectionsOnDrain() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                cluster.get(1).nodetool("drain");

                Assertions.assertThatThrownBy(() -> {
                              SimpleClient newClient = SimpleClient.builder(nativeAddr.getHostString(), 9042).build();
                              newClient.connect(false);
                          }).as("Server should reject new connections after stopAcceptingNewConnections")
                          .isNotNull();
                cluster.get(1).shutdown();
            }
        }
    }

    @Test
    public void testNoEventEmittedWhenDisabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, false))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertThat(subscribedCount).isEqualTo(0);

                boolean disabled = cluster.get(1).callOnInstance(() -> !DatabaseDescriptor.getGracefulDisconnectEnabled());
                Assertions.assertThat(disabled).isTrue();
            }
        }
    }

    @Test
    public void testDrainProceedsImmediatelyWithNoSubscribedConnections() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V4)
                                                   .build())
            {
                client.connect(false);
                cluster.get(1).nodetoolResult("drain").asserts().success();
                cluster.get(1).shutdown();
            }
        }
    }

    @Test
    public void testMultipleConnectionsCanSubscribe() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client1 = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                    .protocolVersion(ProtocolVersion.V5)
                                                    .build();
                 SimpleClient client2 = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                    .protocolVersion(ProtocolVersion.V5)
                                                    .build())
            {
                client1.connect(false);
                client2.connect(false);

                client1.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));
                client2.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertThat(subscribedCount).isEqualTo(2);
            }
        }
    }
}
