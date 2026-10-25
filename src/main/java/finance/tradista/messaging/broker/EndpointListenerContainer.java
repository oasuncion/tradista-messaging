package finance.tradista.messaging.broker;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
 * Manages the live Jakarta Messaging Session and MessageConsumer corresponding
 * to a persisted MessageEndpoint.
 *
 * @author Olivier Asuncion
 */
public class EndpointListenerContainer implements EndpointContainer {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointListenerContainer.class);

	private final MessageEndpoint endpoint;
	private final ConnectionProvider connectionProvider;
	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final ReentrantLock lifecycleLock = new ReentrantLock();

	// Accessed under lifecycleLock.
	private Session session;
	private jakarta.jms.MessageConsumer consumer;

	public EndpointListenerContainer(MessageEndpoint endpoint, ConnectionProvider connectionProvider) {
		this.endpoint = endpoint;
		this.connectionProvider = connectionProvider;
	}

	@Override
	public void start() throws JMSException {
		lifecycleLock.lock();
		try {
			if (running.get() && !paused.get()) {
				return;
			}

			// Recreate both resources on resume; pause() closes the consumer and
			// a fresh session avoids leaking/reusing stale provider state.
			closeResources();
			paused.set(false);
			try {
				initResources();
				running.set(true);
				LOGGER.info("Started/resumed listener container for endpoint {}", endpoint.getName());
			} catch (Exception e) {
				closeResources();
				running.set(false);
				paused.set(false);
				throw asJmsException("Failed to start listener container for endpoint " + endpoint.getName(), e);
			}
		} finally {
			lifecycleLock.unlock();
		}
	}

	private void initResources() throws JMSException {
		Session localSession = null;
		jakarta.jms.MessageConsumer localConsumer = null;
		try {
			MessageFilter<Serializable> filter = TradistaMessagingUtil.createFilterInstance(endpoint.getFilterConfig());
			MessageConsumer<Serializable> applicationConsumer = TradistaMessagingUtil.createConsumerInstance(endpoint);
			if (applicationConsumer == null) {
				throw new IllegalStateException(
						"MessageConsumer could not be instantiated for endpoint " + endpoint.getName());
			}

			Connection connection = connectionProvider.getConnection();
			localSession = connection.createSession(true, Session.SESSION_TRANSACTED);
			Destination destination = connectionProvider.resolveQueue(localSession, endpoint.getQueue().getName());
			localConsumer = localSession.createConsumer(destination);

			DelegatingMessageListener<Serializable> listener = new DelegatingMessageListener<>(endpoint, localSession,
					filter, applicationConsumer, paused, this::onSessionCompromisedAsync);
			localConsumer.setMessageListener(listener);

			// Publish only after all initialization steps succeed.
			session = localSession;
			consumer = localConsumer;
		} catch (Exception e) {
			closeQuietly(localConsumer, localSession);
			throw asJmsException("Failed to initialize listener resources for endpoint " + endpoint.getName(), e);
		}
	}

	/**
	 * Always schedule cleanup away from the JMS delivery thread. The identity check
	 * and state transition happen together under lifecycleLock, preventing a stale
	 * callback from stopping a newer container run.
	 */
	private void onSessionCompromisedAsync(Session compromisedSession) {
		CompletableFuture.runAsync(() -> {
			lifecycleLock.lock();
			try {
				if (session != compromisedSession) {
					return;
				}
				LOGGER.warn("Session compromised for endpoint {}; stopping container resources.", endpoint.getName());
				running.set(false);
				paused.set(false);
				closeResources();
			} finally {
				lifecycleLock.unlock();
			}
		}).exceptionally(e -> {
			LOGGER.error("Asynchronous cleanup failed for endpoint {}", endpoint.getName(), e);
			return null;
		});
	}

	@Override
	public void pause() {
		lifecycleLock.lock();
		try {
			if (!running.get() || paused.get()) {
				return;
			}
			paused.set(true);
			if (consumer != null) {
				try {
					consumer.close();
				} catch (JMSException e) {
					LOGGER.warn("Error closing consumer during pause for endpoint {}", endpoint.getName(), e);
				} finally {
					consumer = null;
				}
			}
			LOGGER.info("Paused listener container for endpoint {}", endpoint.getName());
		} finally {
			lifecycleLock.unlock();
		}
	}

	@Override
	public void stop() {
		lifecycleLock.lock();
		try {
			running.set(false);
			paused.set(false);
			closeResources();
			LOGGER.info("Stopped listener container for endpoint {}", endpoint.getName());
		} finally {
			lifecycleLock.unlock();
		}
	}

	@Override
	public void close() {
		stop();
	}

	private void closeResources() {
		jakarta.jms.MessageConsumer oldConsumer = consumer;
		Session oldSession = session;
		consumer = null;
		session = null;
		closeQuietly(oldConsumer, oldSession);
	}

	private void closeQuietly(jakarta.jms.MessageConsumer oldConsumer, Session oldSession) {
		if (oldConsumer != null) {
			try {
				oldConsumer.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing consumer for endpoint {}", endpoint.getName(), e);
			}
		}
		if (oldSession != null) {
			try {
				oldSession.close();
			} catch (JMSException e) {
				LOGGER.warn("Error closing session for endpoint {}", endpoint.getName(), e);
			}
		}
	}

	private static JMSException asJmsException(String message, Exception cause) {
		if (cause instanceof JMSException jmsException) {
			return jmsException;
		}
		JMSException exception = new JMSException(message + ": " + cause.getMessage());
		exception.initCause(cause);
		return exception;
	}

	public boolean isRunning() {
		return running.get() && !paused.get();
	}

	public boolean isPaused() {
		return paused.get();
	}

	@Override
	public LifecycleStatus getStatus() {
		if (!running.get()) {
			return LifecycleStatus.STOPPED;
		}
		return paused.get() ? LifecycleStatus.PAUSED : LifecycleStatus.RUNNING;
	}

	private static class DelegatingMessageListener<T extends Serializable> implements MessageListener {

		private final MessageEndpoint endpoint;
		private final Session session;
		private final MessageFilter<T> filter;
		private final MessageConsumer<T> consumer;
		private final AtomicBoolean paused;
		private final Consumer<Session> sessionCompromisedCallback;

		private DelegatingMessageListener(MessageEndpoint endpoint, Session session, MessageFilter<T> filter,
				MessageConsumer<T> consumer, AtomicBoolean paused, Consumer<Session> sessionCompromisedCallback) {
			this.endpoint = endpoint;
			this.session = session;
			this.filter = filter;
			this.consumer = consumer;
			this.paused = paused;
			this.sessionCompromisedCallback = sessionCompromisedCallback;
		}

		@Override
		public void onMessage(jakarta.jms.Message brokerMessage) {
			if (paused.get()) {
				LOGGER.debug("Message received while endpoint {} is paused; rolling back.", endpoint.getName());
				rollbackSession();
				return;
			}

			try {
				Message<T> message = MessageConverter.fromBrokerMessage(brokerMessage);
				if (message != null && (filter == null || filter.accept(message.getPayload()))) {
					consumer.onMessage(message);
				}
			} catch (Exception e) {
				LOGGER.error("Application message processing failed for endpoint {}.", endpoint.getName(), e);
				rollbackSession();
				return;
			}

			try {
				session.commit();
			} catch (JMSException commitException) {
				LOGGER.error("Commit failed for endpoint {}; transaction outcome may be uncertain. "
						+ "Invalidating session.", endpoint.getName(), commitException);
				try {
					session.rollback();
				} catch (JMSException rollbackException) {
					LOGGER.warn("Rollback after failed commit also failed for endpoint {}.", endpoint.getName(),
							rollbackException);
				}
				invalidateSession();
			}
		}

		private void rollbackSession() {
			try {
				session.rollback();
			} catch (JMSException e) {
				LOGGER.error("Rollback failed for endpoint {}; invalidating session.", endpoint.getName(), e);
				invalidateSession();
			}
		}

		private void invalidateSession() {
			if (sessionCompromisedCallback != null) {
				sessionCompromisedCallback.accept(session);
			}
		}
	}
}
