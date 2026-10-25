package finance.tradista.messaging.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import finance.tradista.messaging.model.FilterConfig;
import finance.tradista.messaging.model.LifecycleStatus;
import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageQueue;
import finance.tradista.messaging.model.MessageRouter;
import finance.tradista.messaging.model.PollerConfig;
import finance.tradista.messaging.service.MessagingManager;
import finance.tradista.messaging.test.CollectingMessageConsumer;
import finance.tradista.messaging.test.FailingOnceMessageConsumer;
import finance.tradista.messaging.test.PrefixMessageFilter;

/********************************************************************************
 * Copyright (c) 2026 Olivier Asuncion
 * 
 * This program and the accompanying materials are made available under the
 * terms of the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

public class BrokerIntegrationTest {

	private static ActiveMQServer broker;
	private static DefaultConnectionProvider connectionProvider;

	@BeforeAll
	static void startBroker() throws Exception {
		ConfigurationImpl config = new ConfigurationImpl();
		config.setPersistenceEnabled(false);
		config.setSecurityEnabled(false);
		config.setJMXManagementEnabled(false);
		config.addAcceptorConfiguration("in-vm", "vm://0");

		broker = ActiveMQServers.newActiveMQServer(config);
		broker.start();

		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://0");
		connectionProvider = new DefaultConnectionProvider(cf);
		MessagingManager.setConnectionProvider(connectionProvider);
	}

	@AfterAll
	static void stopBroker() throws Exception {
		MessagingManager.setConnectionProvider(null);
		if (connectionProvider != null) {
			connectionProvider.close();
		}
		if (broker != null) {
			broker.stop();
		}
	}

	@BeforeEach
	void setUp() {
		CollectingMessageConsumer.clear();
	}

	@Test
	@DisplayName("Test real Jakarta Messaging broker integration with Router, Filter, and Endpoint Lifecycle")
	void testRealBrokerIntegration() throws Exception {
		// 1. Create Queues in DB
		MessageQueue repoQueue = new MessageQueue("BROKER_REPO_QUEUE");
		MessagingManager.saveQueue(repoQueue);

		MessageQueue fallbackQueue = new MessageQueue("BROKER_DEFAULT_QUEUE");
		MessagingManager.saveQueue(fallbackQueue);

		// 2. Create Filter in DB
		FilterConfig filter = new FilterConfig("BROKER_PREFIX_FILTER", PrefixMessageFilter.class.getName());
		MessagingManager.saveFilterConfig(filter);

		// 3. Create Router in DB
		MessageRouter router = new MessageRouter("BROKER_TRADE_ROUTER");
		router.setDefaultOutputQueue(fallbackQueue);
		router.addSubscription("REPO_TRADE", repoQueue);
		MessagingManager.saveRouter(router);

		// 4. Create Endpoint in DB
		MessageEndpoint endpoint = new MessageEndpoint("BROKER_REPO_ENDPOINT", repoQueue);
		endpoint.setFilterConfig(filter);
		endpoint.setConsumerClass(CollectingMessageConsumer.class.getName());
		MessagingManager.saveEndpoint(endpoint);

		// Initially STOPPED: Start the endpoint
		MessagingManager.startEndpoint("BROKER_REPO_ENDPOINT");
		assertEquals(LifecycleStatus.RUNNING, MessagingManager.getEndpointByName("BROKER_REPO_ENDPOINT").getStatus());

		// 5. Publish events through real broker
		CollectingMessageConsumer.resetLatch(2);

		// Matching filter:
		MessagingManager.publishEvent("BROKER_TRADE_ROUTER", "REPO_TRADE", new Message<>("EXPORT_TRADE_BROKER_1"));
		// Rejected by filter:
		MessagingManager.publishEvent("BROKER_TRADE_ROUTER", "REPO_TRADE", new Message<>("IGNORE_TRADE_BROKER_2"));
		// Matching filter:
		MessagingManager.publishEvent("BROKER_TRADE_ROUTER", "REPO_TRADE", new Message<>("EXPORT_TRADE_BROKER_3"));

		// Wait deterministically for asynchronous listener delivery
		assertTrue(CollectingMessageConsumer.await(5, TimeUnit.SECONDS),
				"Expected 2 messages to be delivered within 5 seconds");

		// Verify that exactly 2 messages were accepted by the filter and consumed
		// asynchronously
		assertEquals(2, CollectingMessageConsumer.getReceived().size());
		assertEquals("EXPORT_TRADE_BROKER_1", CollectingMessageConsumer.getReceived().get(0).getPayload());
		assertEquals("EXPORT_TRADE_BROKER_3", CollectingMessageConsumer.getReceived().get(1).getPayload());

		// Pause endpoint
		MessagingManager.pauseEndpoint("BROKER_REPO_ENDPOINT");
		assertEquals(LifecycleStatus.PAUSED, MessagingManager.getEndpointByName("BROKER_REPO_ENDPOINT").getStatus());

		// Stop endpoint
		MessagingManager.stopEndpoint("BROKER_REPO_ENDPOINT");
		assertEquals(LifecycleStatus.STOPPED, MessagingManager.getEndpointByName("BROKER_REPO_ENDPOINT").getStatus());

		// 6. Test Poller Endpoint on real broker
		CollectingMessageConsumer.clear();
		CollectingMessageConsumer.resetLatch(1);

		MessageQueue pollerQueue = new MessageQueue("BROKER_POLLER_QUEUE");
		MessagingManager.saveQueue(pollerQueue);

		MessageEndpoint pollerEndpoint = new MessageEndpoint("BROKER_POLLER_ENDPOINT", pollerQueue);
		pollerEndpoint.setPollerConfig(new PollerConfig(100L, 5)); // polls every 100ms
		pollerEndpoint.setConsumerClass(CollectingMessageConsumer.class.getName());
		MessagingManager.saveEndpoint(pollerEndpoint);

		// Send message before starting poller
		MessagingManager.send("BROKER_POLLER_QUEUE", new Message<>("POLLER_PAYLOAD_1"));

		// Start poller: it will periodically drain the queue
		MessagingManager.startEndpoint("BROKER_POLLER_ENDPOINT");
		assertEquals(LifecycleStatus.RUNNING, MessagingManager.getEndpointByName("BROKER_POLLER_ENDPOINT").getStatus());

		assertTrue(CollectingMessageConsumer.await(5, TimeUnit.SECONDS),
				"Expected 1 polled message to be delivered within 5 seconds");
		assertEquals(1, CollectingMessageConsumer.getReceived().size());
		assertEquals("POLLER_PAYLOAD_1", CollectingMessageConsumer.getReceived().get(0).getPayload());

		// Stop poller endpoint
		MessagingManager.stopEndpoint("BROKER_POLLER_ENDPOINT");
		assertEquals(LifecycleStatus.STOPPED, MessagingManager.getEndpointByName("BROKER_POLLER_ENDPOINT").getStatus());

		// 7. Test Rollback and Redelivery on failure (Zero Message Loss)
		FailingOnceMessageConsumer.reset(2);
		MessageQueue retryQueue = new MessageQueue("BROKER_RETRY_QUEUE");
		MessagingManager.saveQueue(retryQueue);

		MessageEndpoint retryEndpoint = new MessageEndpoint("BROKER_RETRY_ENDPOINT", retryQueue);
		retryEndpoint.setPollerConfig(new PollerConfig(100L, 1));
		retryEndpoint.setConsumerClass(FailingOnceMessageConsumer.class.getName());
		MessagingManager.saveEndpoint(retryEndpoint);

		MessagingManager.send("BROKER_RETRY_QUEUE", new Message<>("RETRY_PAYLOAD"));

		// Start poller: first poll fails and triggers session.rollback(), second poll
		// succeeds
		MessagingManager.startEndpoint("BROKER_RETRY_ENDPOINT");

		assertTrue(FailingOnceMessageConsumer.await(5, TimeUnit.SECONDS),
				"Message should have been redelivered after rollback on failure");
		assertEquals(2, FailingOnceMessageConsumer.getAttempts());

		MessagingManager.stopEndpoint("BROKER_RETRY_ENDPOINT");
	}
}
