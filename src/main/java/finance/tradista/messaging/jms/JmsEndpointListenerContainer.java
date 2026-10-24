package finance.tradista.messaging.jms;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import finance.tradista.messaging.model.LifecycleStatus;
import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageConsumer;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageFilter;
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
 * Manages the live JMS Session and MessageConsumer corresponding to a persisted MessageEndpoint.
 * Controls the active/paused/stopped lifecycle directly on the underlying JMS consumer.
 * 
 * @author Olivier Asuncion
 */
public class JmsEndpointListenerContainer implements AutoCloseable {

	private static final Logger LOGGER = LogManager.getLogger(JmsEndpointListenerContainer.class);

	private final MessageEndpoint endpoint;
	private final JmsConnectionProvider connectionProvider;
	private Session session;
	private jakarta.jms.MessageConsumer jmsConsumer;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final AtomicBoolean paused = new AtomicBoolean(false);

	public JmsEndpointListenerContainer(MessageEndpoint endpoint, JmsConnectionProvider connectionProvider) {
		this.endpoint = endpoint;
		this.connectionProvider = connectionProvider;
	}

	public synchronized void start() throws JMSException {
		if (running.get() && !paused.get()) {
			return;
		}

		if (paused.get()) {
			paused.set(false);
			running.set(true);
			LOGGER.info("Resumed JMS listener container for endpoint {}", endpoint.getName());
			return;
		}

		Connection conn = connectionProvider.getConnection();
		session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = connectionProvider.resolveQueue(session, endpoint.getQueue().getName());

		jmsConsumer = session.createConsumer(dest);

		@SuppressWarnings("unchecked")
		MessageConsumer<Object> consumer = (MessageConsumer<Object>) endpoint.getConsumerInstance();
		@SuppressWarnings("unchecked")
		MessageFilter<Object> filter = endpoint.getFilterConfig() != null
				? (MessageFilter<Object>) endpoint.getFilterConfig().getInstance()
				: null;

		jmsConsumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(jakarta.jms.Message message) {
				if (paused.get() || !running.get()) {
					LOGGER.debug("Message received while endpoint {} is paused/stopped. Ignored or unhandled.",
							endpoint.getName());
					return;
				}
				try {
					Message<Object> tradistaMsg = JmsMessageConverter.fromJmsMessage(message);
					if (filter != null && !filter.accept(tradistaMsg.getPayload())) {
						LOGGER.debug("Message {} rejected by filter {} for endpoint {}", tradistaMsg.getId(),
								endpoint.getFilterConfig().getName(), endpoint.getName());
						return;
					}
					if (consumer != null) {
						consumer.onMessage(tradistaMsg);
					}
				} catch (Exception e) {
					LOGGER.error("Error processing message in endpoint {}: {}", endpoint.getName(), e.getMessage(), e);
				}
			}
		});

		running.set(true);
		paused.set(false);
		LOGGER.info("Started JMS listener container for endpoint {}", endpoint.getName());
	}

	public synchronized void pause() {
		paused.set(true);
		LOGGER.info("Paused JMS listener container for endpoint {}", endpoint.getName());
	}

	public synchronized void stop() {
		running.set(false);
		paused.set(false);
		closeConsumer();
		LOGGER.info("Stopped JMS listener container for endpoint {}", endpoint.getName());
	}

	private void closeConsumer() {
		if (jmsConsumer != null) {
			try {
				jmsConsumer.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing JMS consumer for endpoint {}: {}", endpoint.getName(), e.getMessage());
			}
			jmsConsumer = null;
		}
		if (session != null) {
			try {
				session.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing JMS session for endpoint {}: {}", endpoint.getName(), e.getMessage());
			}
			session = null;
		}
	}

	@Override
	public void close() {
		stop();
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
}
