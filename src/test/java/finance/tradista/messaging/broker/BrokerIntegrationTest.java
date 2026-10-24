package finance.tradista.messaging.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import finance.tradista.messaging.service.MessagingManager;
import finance.tradista.messaging.test.CollectingMessageConsumer;
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

		// 5. Route messages through real broker
		// Matching filter:
		MessagingManager.route(router, new Message<>("EXPORT_TRADE_BROKER_1"), "REPO_TRADE");
		// Rejected by filter:
		MessagingManager.route(router, new Message<>("IGNORE_TRADE_BROKER_2"), "REPO_TRADE");
		// Matching filter:
		MessagingManager.route(router, new Message<>("EXPORT_TRADE_BROKER_3"), "REPO_TRADE");

		// Wait briefly for asynchronous listener delivery
		for (int i = 0; i < 20; i++) {
			if (CollectingMessageConsumer.getReceived().size() >= 2) {
				break;
			}
			TimeUnit.MILLISECONDS.sleep(100);
		}

		// Verify that exactly 2 messages were accepted by the filter and consumed asynchronously
		assertEquals(2, CollectingMessageConsumer.getReceived().size());
		assertEquals("EXPORT_TRADE_BROKER_1", CollectingMessageConsumer.getReceived().get(0).getPayload());
		assertEquals("EXPORT_TRADE_BROKER_3", CollectingMessageConsumer.getReceived().get(1).getPayload());

		// Pause endpoint
		MessagingManager.pauseEndpoint("BROKER_REPO_ENDPOINT");
		assertEquals(LifecycleStatus.PAUSED, MessagingManager.getEndpointByName("BROKER_REPO_ENDPOINT").getStatus());

		// Stop endpoint
		MessagingManager.stopEndpoint("BROKER_REPO_ENDPOINT");
		assertEquals(LifecycleStatus.STOPPED, MessagingManager.getEndpointByName("BROKER_REPO_ENDPOINT").getStatus());
	}
}
