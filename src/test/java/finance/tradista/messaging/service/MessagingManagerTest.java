package finance.tradista.messaging.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import finance.tradista.messaging.exception.TradistaMessagingBusinessException;
import finance.tradista.messaging.model.FilterConfig;
import finance.tradista.messaging.model.LifecycleStatus;
import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageFilter;
import finance.tradista.messaging.model.MessageQueue;
import finance.tradista.messaging.model.MessageRouter;
import finance.tradista.messaging.model.PollerConfig;
import finance.tradista.messaging.test.CollectingMessageConsumer;
import finance.tradista.messaging.test.PrefixMessageFilter;
import finance.tradista.messaging.util.TradistaMessagingUtil;

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

public class MessagingManagerTest {

	@BeforeEach
	void setUp() {
		CollectingMessageConsumer.clear();
	}

	@Test
	@DisplayName("Save invalid queue")
	void testSaveInvalidQueue() {
		assertThrows(TradistaMessagingBusinessException.class, () -> MessagingManager.saveQueue(null));

		MessageQueue queue = new MessageQueue();
		assertThrows(TradistaMessagingBusinessException.class, () -> MessagingManager.saveQueue(queue));

		queue.setName(" ");
		assertThrows(TradistaMessagingBusinessException.class, () -> MessagingManager.saveQueue(queue));
	}

	@Test
	@DisplayName("Save and retrieve queue")
	void testSaveAndRetrieveQueue() throws TradistaMessagingBusinessException {
		MessageQueue queue = new MessageQueue("REPO_QUEUE");
		queue.setDescription("Queue for repo trade events");
		queue.setCapacity(500);

		long id = MessagingManager.saveQueue(queue);
		assertTrue(id > 0);

		MessageQueue fetched = MessagingManager.getQueueByName("REPO_QUEUE");
		assertNotNull(fetched);
		assertEquals("REPO_QUEUE", fetched.getName());
		assertEquals("REPO_QUEUE", fetched.getQueueName());
		assertEquals(500, fetched.getCapacity());

		// Test unique constraint
		MessageQueue duplicate = new MessageQueue("REPO_QUEUE");
		assertThrows(TradistaMessagingBusinessException.class, () -> MessagingManager.saveQueue(duplicate));
	}

	@Test
	@DisplayName("Save and retrieve filter config")
	void testSaveAndRetrieveFilterConfig() throws TradistaMessagingBusinessException {
		FilterConfig filter = new FilterConfig("PREFIX_FILTER", PrefixMessageFilter.class.getName());
		filter.setDescription("Filter by EXPORT_ prefix");

		long id = MessagingManager.saveFilterConfig(filter);
		assertTrue(id > 0);

		FilterConfig fetched = MessagingManager.getFilterConfigByName("PREFIX_FILTER");
		assertNotNull(fetched);
		assertEquals(PrefixMessageFilter.class.getName(), fetched.getFilterClass());
		MessageFilter<String> filterInstance = TradistaMessagingUtil.createFilterInstance(fetched);
		assertNotNull(filterInstance);
		assertTrue(filterInstance.accept("EXPORT_TRADE_123"));
		assertFalse(filterInstance.accept("IMPORT_TRADE_123"));
	}

	@Test
	@DisplayName("Save and test message router")
	void testSaveAndTestRouter() throws TradistaMessagingBusinessException {
		MessageQueue fixQueue = new MessageQueue("FIX_QUEUE");
		MessagingManager.saveQueue(fixQueue);

		MessageQueue transferQueue = new MessageQueue("TRANSFER_QUEUE");
		MessagingManager.saveQueue(transferQueue);

		MessageQueue defaultQueue = new MessageQueue("DEFAULT_QUEUE");
		MessagingManager.saveQueue(defaultQueue);

		MessageRouter router = new MessageRouter("TRADE_EVENT_ROUTER");
		router.setDefaultOutputQueue(defaultQueue);
		// 1-to-N subscriptions: REPO_TRADE dispatched to both FIX_QUEUE and TRANSFER_QUEUE
		router.addSubscription("REPO_TRADE", fixQueue);
		router.addSubscription("REPO_TRADE", transferQueue);

		long id = MessagingManager.saveRouter(router);
		assertTrue(id > 0);

		MessageRouter fetched = MessagingManager.getRouterByName("TRADE_EVENT_ROUTER");
		assertNotNull(fetched);
		assertEquals(2, fetched.getRules().size());

		// Test Routing logic (1-to-N)
		Message<String> repoMsg = new Message<>("REPO_TRADE_001");
		MessagingManager.route(fetched, repoMsg, "REPO_TRADE");

		Message<String> receivedOnFix = MessagingManager.receive("FIX_QUEUE");
		assertNotNull(receivedOnFix);
		assertEquals("REPO_TRADE_001", receivedOnFix.getPayload());

		Message<String> receivedOnTransfer = MessagingManager.receive("TRANSFER_QUEUE");
		assertNotNull(receivedOnTransfer);
		assertEquals("REPO_TRADE_001", receivedOnTransfer.getPayload());

		// Test publishEvent API
		Message<String> publishedMsg = new Message<>("REPO_TRADE_002");
		MessagingManager.publishEvent("TRADE_EVENT_ROUTER", "REPO_TRADE", publishedMsg);
		assertNotNull(MessagingManager.receive("FIX_QUEUE"));
		assertNotNull(MessagingManager.receive("TRANSFER_QUEUE"));

		// Test default routing
		Message<String> unknownMsg = new Message<>("UNKNOWN_EVENT");
		MessagingManager.route(fetched, unknownMsg, "OTHER_TRADE");

		Message<String> receivedOnDefault = MessagingManager.receive("DEFAULT_QUEUE");
		assertNotNull(receivedOnDefault);
		assertEquals("UNKNOWN_EVENT", receivedOnDefault.getPayload());
	}

	@Test
	@DisplayName("Save and manage endpoint lifecycle and polling")
	void testEndpointLifecycleAndPolling() throws TradistaMessagingBusinessException {
		MessageQueue queue = new MessageQueue("ENDPOINT_QUEUE");
		MessagingManager.saveQueue(queue);

		FilterConfig filter = new FilterConfig("ENDPOINT_FILTER", PrefixMessageFilter.class.getName());
		MessagingManager.saveFilterConfig(filter);

		MessageEndpoint endpoint = new MessageEndpoint("FIX_EXPORT_ENDPOINT", queue);
		endpoint.setFilterConfig(filter);
		endpoint.setConsumerClass(CollectingMessageConsumer.class.getName());
		endpoint.setPollerConfig(new PollerConfig(500, 5));

		long id = MessagingManager.saveEndpoint(endpoint);
		assertTrue(id > 0);

		MessageEndpoint fetched = MessagingManager.getEndpointByName("FIX_EXPORT_ENDPOINT");
		assertNotNull(fetched);
		assertEquals(LifecycleStatus.STOPPED, fetched.getStatus());

		// Send 2 matching messages and 1 non-matching message
		MessagingManager.send("ENDPOINT_QUEUE", new Message<>("EXPORT_TRADE_1"));
		MessagingManager.send("ENDPOINT_QUEUE", new Message<>("IGNORED_TRADE_2"));
		MessagingManager.send("ENDPOINT_QUEUE", new Message<>("EXPORT_TRADE_3"));

		// When STOPPED, polling should process 0 messages
		int processedWhileStopped = MessagingManager.pollEndpoint("FIX_EXPORT_ENDPOINT");
		assertEquals(0, processedWhileStopped);
		assertEquals(0, CollectingMessageConsumer.getReceived().size());

		// Start endpoint
		MessagingManager.startEndpoint("FIX_EXPORT_ENDPOINT");
		MessageEndpoint running = MessagingManager.getEndpointByName("FIX_EXPORT_ENDPOINT");
		assertEquals(LifecycleStatus.RUNNING, running.getStatus());

		// Poll endpoint
		int processed = MessagingManager.pollEndpoint("FIX_EXPORT_ENDPOINT");
		// 3 messages in queue: 2 matched, 1 filtered out
		assertEquals(2, processed);
		assertEquals(2, CollectingMessageConsumer.getReceived().size());
		assertEquals("EXPORT_TRADE_1", CollectingMessageConsumer.getReceived().get(0).getPayload());
		assertEquals("EXPORT_TRADE_3", CollectingMessageConsumer.getReceived().get(1).getPayload());

		// Pause endpoint
		MessagingManager.pauseEndpoint("FIX_EXPORT_ENDPOINT");
		MessageEndpoint paused = MessagingManager.getEndpointByName("FIX_EXPORT_ENDPOINT");
		assertEquals(LifecycleStatus.PAUSED, paused.getStatus());

		// Stop endpoint
		MessagingManager.stopEndpoint("FIX_EXPORT_ENDPOINT");
		MessageEndpoint stopped = MessagingManager.getEndpointByName("FIX_EXPORT_ENDPOINT");
		assertEquals(LifecycleStatus.STOPPED, stopped.getStatus());
	}
}
