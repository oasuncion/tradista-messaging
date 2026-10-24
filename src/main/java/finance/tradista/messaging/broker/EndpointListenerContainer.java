package finance.tradista.messaging.broker;

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
import jakarta.jms.MessageListener;
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
 * Manages the live Jakarta Messaging Session and MessageConsumer corresponding to a persisted MessageEndpoint.
 * Controls the active/paused/stopped lifecycle directly on the underlying consumer.
 * 
 * @author Olivier Asuncion
 */
public class EndpointListenerContainer implements AutoCloseable {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointListenerContainer.class);

	private final MessageEndpoint endpoint;
	private final ConnectionProvider connectionProvider;

	private Session session;
	private jakarta.jms.MessageConsumer consumer;

	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final AtomicBoolean running = new AtomicBoolean(false);

	public EndpointListenerContainer(MessageEndpoint endpoint, ConnectionProvider connectionProvider) {
		this.endpoint = endpoint;
		this.connectionProvider = connectionProvider;
	}

	public synchronized void start() throws JMSException {
		if (running.get() && !paused.get()) {
			return;
		}

		if (paused.get()) {
			paused.set(false);
			LOGGER.info("Resumed listener container for endpoint {}", endpoint.getName());
			return;
		}

		Connection connection = connectionProvider.getConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = connectionProvider.resolveQueue(session, endpoint.getQueue().getName());
		consumer = session.createConsumer(destination);

		consumer.setMessageListener(new DelegatingMessageListener<>(endpoint, paused));
		running.set(true);
		paused.set(false);
		LOGGER.info("Started listener container for endpoint {}", endpoint.getName());
	}

	public synchronized void pause() {
		paused.set(true);
		LOGGER.info("Paused listener container for endpoint {}", endpoint.getName());
	}

	public synchronized void stop() {
		running.set(false);
		paused.set(false);
		closeResources();
		LOGGER.info("Stopped listener container for endpoint {}", endpoint.getName());
	}

	@Override
	public synchronized void close() {
		stop();
	}

	private void closeResources() {
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

	public boolean isRunning() {
		return running.get() && !paused.get();
	}

	public boolean isPaused() {
		return paused.get();
	}

	public LifecycleStatus getStatus() {
		if (!running.get()) {
			return LifecycleStatus.STOPPED;
		}
		if (paused.get()) {
			return LifecycleStatus.PAUSED;
		}
		return LifecycleStatus.RUNNING;
	}

	private static class DelegatingMessageListener<T extends java.io.Serializable> implements MessageListener {

		private final MessageEndpoint endpoint;
		private final AtomicBoolean paused;

		public DelegatingMessageListener(MessageEndpoint endpoint, AtomicBoolean paused) {
			this.endpoint = endpoint;
			this.paused = paused;
		}

		@Override
		public void onMessage(jakarta.jms.Message message) {
			if (paused.get()) {
				LOGGER.debug("Message ignored because endpoint {} is paused.", endpoint.getName());
				return;
			}

			try {
				Message<T> msg = MessageConverter.fromBrokerMessage(message);
				if (msg == null) {
					return;
				}

				MessageFilter<T> filter = TradistaMessagingUtil.createFilterInstance(endpoint.getFilterConfig());

				if (filter == null || filter.accept(msg.getPayload())) {
					MessageConsumer<T> consumer = TradistaMessagingUtil.createConsumerInstance(endpoint);
					if (consumer != null) {
						consumer.onMessage(msg);
					}
				}
			} catch (Exception e) {
				LOGGER.error("Error processing message for endpoint {}: {}", endpoint.getName(), e.getMessage(), e);
			}
		}
	}
}
