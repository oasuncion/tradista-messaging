package finance.tradista.messaging.broker;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import finance.tradista.messaging.model.LifecycleStatus;
import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageConsumer;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageFilter;
import finance.tradista.messaging.util.TradistaMessagingUtil;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

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

/**
 * Scheduled batch-polling container for endpoints configured with a
 * PollerConfig. Periodically drains up to maxMessagesPerPoll from the
 * destination queue.
 * 
 * @author Olivier Asuncion
 */
public class EndpointPollerContainer implements EndpointContainer {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointPollerContainer.class);

	private final MessageEndpoint endpoint;
	private final ConnectionProvider connectionProvider;

	private Session session;
	private jakarta.jms.MessageConsumer consumer;

	private ScheduledExecutorService scheduler;
	private ScheduledFuture<?> scheduledTask;

	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final AtomicBoolean running = new AtomicBoolean(false);

	public EndpointPollerContainer(MessageEndpoint endpoint, ConnectionProvider connectionProvider) {
		this.endpoint = endpoint;
		this.connectionProvider = connectionProvider;
	}

	@Override
	public synchronized void start() throws JMSException {
		if (running.get() && !paused.get()) {
			return;
		}

		if (paused.get()) {
			paused.set(false);
			LOGGER.info("Resumed poller container for endpoint {}", endpoint.getName());
			return;
		}

		Connection connection = connectionProvider.getConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = connectionProvider.resolveQueue(session, endpoint.getQueue().getName());
		consumer = session.createConsumer(destination);

		long delayMs = endpoint.getPollerConfig() != null ? endpoint.getPollerConfig().getDelayMs() : 1000L;
		if (delayMs <= 0) {
			delayMs = 1000L;
		}

		scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "poller-" + endpoint.getName());
			t.setDaemon(true);
			return t;
		});

		scheduledTask = scheduler.scheduleWithFixedDelay(this::pollBatch, delayMs, delayMs, TimeUnit.MILLISECONDS);

		running.set(true);
		paused.set(false);
		LOGGER.info("Started poller container for endpoint {} with delay {} ms", endpoint.getName(), delayMs);
	}

	@Override
	public synchronized void pause() {
		paused.set(true);
		LOGGER.info("Paused poller container for endpoint {}", endpoint.getName());
	}

	@Override
	public synchronized void stop() {
		running.set(false);
		paused.set(false);
		closeResources();
		LOGGER.info("Stopped poller container for endpoint {}", endpoint.getName());
	}

	@Override
	public synchronized void close() {
		stop();
	}

	private void closeResources() {
		if (scheduledTask != null) {
			scheduledTask.cancel(false);
			scheduledTask = null;
		}
		if (scheduler != null) {
			scheduler.shutdownNow();
			scheduler = null;
		}
		if (consumer != null) {
			try {
				consumer.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing consumer for endpoint {}: {}", endpoint.getName(), e.getMessage());
			}
			consumer = null;
		}
		if (session != null) {
			try {
				session.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing session for endpoint {}: {}", endpoint.getName(), e.getMessage());
			}
			session = null;
		}
	}

	private void pollBatch() {
		if (paused.get() || !running.get() || consumer == null) {
			return;
		}

		int maxPoll = endpoint.getPollerConfig() != null ? endpoint.getPollerConfig().getMaxMessagesPerPoll() : 10;
		if (maxPoll <= 0) {
			maxPoll = 10;
		}

		MessageFilter<Serializable> filter = TradistaMessagingUtil.createFilterInstance(endpoint.getFilterConfig());
		MessageConsumer<Serializable> msgConsumer = TradistaMessagingUtil.createConsumerInstance(endpoint);

		if (msgConsumer == null) {
			return;
		}

		for (int i = 0; i < maxPoll; i++) {
			try {
				jakarta.jms.Message brokerMessage = consumer.receiveNoWait();
				if (brokerMessage == null) {
					break;
				}

				Message<Serializable> msg = MessageConverter.fromBrokerMessage(brokerMessage);
				if (msg == null) {
					continue;
				}

				if (filter == null || filter.accept(msg.getPayload())) {
					msgConsumer.onMessage(msg);
				}
			} catch (Exception e) {
				LOGGER.error("Error during polling for endpoint {}: {}", endpoint.getName(), e.getMessage(), e);
				break;
			}
		}
	}

	@Override
	public boolean isRunning() {
		return running.get() && !paused.get();
	}

	@Override
	public boolean isPaused() {
		return paused.get();
	}

	@Override
	public LifecycleStatus getStatus() {
		if (!running.get()) {
			return LifecycleStatus.STOPPED;
		}
		if (paused.get()) {
			return LifecycleStatus.PAUSED;
		}
		return LifecycleStatus.RUNNING;
	}
}
