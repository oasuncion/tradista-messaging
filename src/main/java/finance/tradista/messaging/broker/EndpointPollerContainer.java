package finance.tradista.messaging.broker;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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
 * PollerConfig.
 *
 * @author Olivier Asuncion
 */
public class EndpointPollerContainer implements EndpointContainer {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointPollerContainer.class);

	private final MessageEndpoint endpoint;
	private final ConnectionProvider connectionProvider;

	// JMS resources are accessed only while cycleLock is held.
	private Session session;
	private jakarta.jms.MessageConsumer consumer;

	private MessageFilter<Serializable> filter;
	private MessageConsumer<Serializable> messageConsumer;

	// Accessed under lifecycleLock.
	private ScheduledExecutorService scheduler;
	private ScheduledFuture<?> scheduledTask;

	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final ReentrantLock lifecycleLock = new ReentrantLock();
	private final ReentrantLock cycleLock = new ReentrantLock();

	public EndpointPollerContainer(MessageEndpoint endpoint, ConnectionProvider connectionProvider) {
		this.endpoint = endpoint;
		this.connectionProvider = connectionProvider;
	}

	@Override
	public void start() throws JMSException {
		boolean cycleLocked = false;
		try {
			// Lock order is cycleLock -> lifecycleLock. Never wait for cycleLock
			// while holding lifecycleLock, because application callbacks may call stop().
			cycleLocked = cycleLock.tryLock(2, TimeUnit.SECONDS);
			if (!cycleLocked) {
				throw new JMSException(
						"Cannot start endpoint " + endpoint.getName() + ": a previous polling cycle is still active.");
			}

			lifecycleLock.lock();
			try {
				if (running.get() && !paused.get()) {
					return;
				}
				if (running.get() && paused.get()) {
					paused.set(false);
					LOGGER.info("Resumed poller container for endpoint {}", endpoint.getName());
					return;
				}

				closeJmsResources();
				filter = TradistaMessagingUtil.createFilterInstance(endpoint.getFilterConfig());
				messageConsumer = TradistaMessagingUtil.createConsumerInstance(endpoint);
				if (messageConsumer == null) {
					throw new IllegalStateException(
							"MessageConsumer could not be instantiated for endpoint " + endpoint.getName());
				}

				long delayMs = endpoint.getPollerConfig() != null ? endpoint.getPollerConfig().getDelayMs() : 1000L;
				if (delayMs <= 0) {
					delayMs = 1000L;
				}

				initJmsResources();

				scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
					Thread thread = new Thread(r, "poller-" + endpoint.getName());
					thread.setDaemon(true);
					return thread;
				});

				scheduledTask = scheduler.scheduleWithFixedDelay(this::pollBatch, delayMs, delayMs,
						TimeUnit.MILLISECONDS);

				paused.set(false);
				running.set(true);
				LOGGER.info("Started poller container for endpoint {} with delay {} ms", endpoint.getName(), delayMs);
			} catch (Exception e) {
				running.set(false);
				paused.set(false);
				closeJmsResources();
				shutdownSchedulerWithoutWaiting();
				throw asJmsException("Failed to start poller container for endpoint " + endpoint.getName(), e);
			} finally {
				lifecycleLock.unlock();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw asJmsException(
					"Interrupted while waiting for endpoint " + endpoint.getName() + " polling cycle to finish", e);
		} finally {
			if (cycleLocked) {
				cycleLock.unlock();
			}
		}
	}

	@Override
	public void pause() {
		lifecycleLock.lock();
		try {
			if (!running.get() || paused.get()) {
				return;
			}
			paused.set(true);
		} finally {
			lifecycleLock.unlock();
		}

		// Wait for the in-flight transaction without holding lifecycleLock.
		if (!cycleLock.isHeldByCurrentThread()) {
			boolean locked = false;
			try {
				locked = cycleLock.tryLock(2, TimeUnit.SECONDS);
				if (!locked) {
					LOGGER.warn("Pause requested for endpoint {}, but the active cycle is still running; "
							+ "the poller will pause at its next checkpoint.", endpoint.getName());
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.warn("Interrupted while waiting for endpoint {} polling cycle to pause.", endpoint.getName(), e);
			} finally {
				if (locked) {
					cycleLock.unlock();
				}
			}
		}
		LOGGER.info("Paused poller container for endpoint {}", endpoint.getName());
	}

	@Override
	public void stop() {
		// If stop() is called by application code from inside pollBatch(), do not
		// reentrantly close the session in the middle of the current transaction.
		boolean calledFromCycle = cycleLock.isHeldByCurrentThread();
		ScheduledExecutorService schedulerToStop;

		lifecycleLock.lock();
		try {
			running.set(false);
			paused.set(false);
			if (scheduledTask != null) {
				scheduledTask.cancel(false);
				scheduledTask = null;
			}
			schedulerToStop = scheduler;
			scheduler = null;
		} finally {
			lifecycleLock.unlock();
		}

		if (schedulerToStop != null) {
			schedulerToStop.shutdown();
			if (!calledFromCycle && !isCurrentPollerThread()) {
				try {
					if (!schedulerToStop.awaitTermination(2, TimeUnit.SECONDS)) {
						LOGGER.warn("Polling task for endpoint {} did not finish within 2000ms.", endpoint.getName());
						schedulerToStop.shutdownNow();
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					schedulerToStop.shutdownNow();
				}
			}
		}

		if (calledFromCycle) {
			// pollBatch() closes resources in its finally block once the callback exits.
			LOGGER.debug("Stop requested from the active poll cycle for endpoint {}; cleanup is deferred.",
					endpoint.getName());
			return;
		}

		boolean locked = false;
		try {
			locked = cycleLock.tryLock(2, TimeUnit.SECONDS);
			if (locked) {
				closeJmsResources();
			} else {
				LOGGER.warn("Polling cycle for endpoint {} is still active; cleanup will be performed "
						+ "when that cycle exits.", endpoint.getName());
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.warn("Interrupted while waiting to close JMS resources for endpoint {}; "
					+ "cleanup will be performed by the active cycle.", endpoint.getName(), e);
		} finally {
			if (locked) {
				cycleLock.unlock();
			}
		}
		LOGGER.info("Stopped poller container for endpoint {}", endpoint.getName());
	}

	@Override
	public void close() {
		stop();
	}

	private void initJmsResources() throws JMSException {
		Session localSession = null;
		jakarta.jms.MessageConsumer localConsumer = null;
		try {
			Connection connection = connectionProvider.getConnection();
			localSession = connection.createSession(true, Session.SESSION_TRANSACTED);
			Destination destination = connectionProvider.resolveQueue(localSession, endpoint.getQueue().getName());
			localConsumer = localSession.createConsumer(destination);

			// Publish only when the entire resource chain is ready.
			session = localSession;
			consumer = localConsumer;
		} catch (Exception e) {
			closeQuietly(localConsumer, localSession);
			throw asJmsException("Failed to initialize JMS resources for endpoint " + endpoint.getName(), e);
		}
	}

	private void closeJmsResources() {
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
				LOGGER.debug("Ignored exception closing consumer for endpoint {}.", endpoint.getName(), e);
			}
		}
		if (oldSession != null) {
			try {
				oldSession.close();
			} catch (JMSException e) {
				LOGGER.debug("Ignored exception closing session for endpoint {}.", endpoint.getName(), e);
			}
		}
	}

	private void shutdownSchedulerWithoutWaiting() {
		if (scheduledTask != null) {
			scheduledTask.cancel(false);
			scheduledTask = null;
		}
		if (scheduler != null) {
			scheduler.shutdownNow();
			scheduler = null;
		}
	}

	private boolean isCurrentPollerThread() {
		return Thread.currentThread().getName().equals("poller-" + endpoint.getName());
	}

	private void pollBatch() {
		if (paused.get() || !running.get()) {
			return;
		}
		if (!cycleLock.tryLock()) {
			return;
		}

		try {
			if (!running.get() || paused.get()) {
				return;
			}

			if (consumer == null || session == null) {
				try {
					closeJmsResources();
					initJmsResources();
					LOGGER.info("Recovered JMS resources for endpoint {}", endpoint.getName());
				} catch (Exception e) {
					LOGGER.warn("JMS resources unavailable for endpoint {}; retrying next cycle: {}",
							endpoint.getName(), e.getMessage(), e);
					return;
				}
			}

			int maxPoll = endpoint.getPollerConfig() != null ? endpoint.getPollerConfig().getMaxMessagesPerPoll() : 10;
			if (maxPoll <= 0) {
				maxPoll = 10;
			}

			for (int i = 0; i < maxPoll; i++) {
				if (!running.get() || paused.get()) {
					break;
				}

				jakarta.jms.Message brokerMessage;
				try {
					brokerMessage = consumer.receiveNoWait();
				} catch (JMSException e) {
					LOGGER.error("JMS receive failed for endpoint {}; invalidating resources.", endpoint.getName(), e);
					closeJmsResources();
					return;
				}

				if (brokerMessage == null) {
					break;
				}

				try {
					Message<Serializable> message = MessageConverter.fromBrokerMessage(brokerMessage);
					if (message != null && (filter == null || filter.accept(message.getPayload()))) {
						messageConsumer.onMessage(message);
					}
				} catch (Exception e) {
					LOGGER.error("Application message processing failed for endpoint {}.", endpoint.getName(), e);
					rollbackSession();
					break;
				}

				// Do not commit work if stop() was requested during application processing.
				if (!running.get()) {
					rollbackSession();
					break;
				}

				try {
					session.commit();
				} catch (JMSException commitException) {
					LOGGER.error("JMS commit failed for endpoint {}; invalidating resources.", endpoint.getName(),
							commitException);
					try {
						session.rollback();
					} catch (JMSException rollbackException) {
						LOGGER.warn("Rollback after failed commit also failed for endpoint {}.", endpoint.getName(),
								rollbackException);
					}
					closeJmsResources();
					break;
				}
			}
		} catch (Exception e) {
			LOGGER.error("Unexpected error in poller batch task for endpoint {}.", endpoint.getName(), e);
		} finally {
			try {
				// This also handles stop() timing out while waiting for an active cycle.
				if (!running.get()) {
					closeJmsResources();
				}
			} finally {
				cycleLock.unlock();
			}
		}
	}

	private void rollbackSession() {
		try {
			if (session != null) {
				session.rollback();
			}
		} catch (JMSException e) {
			LOGGER.error("Rollback failed for endpoint {}; invalidating resources.", endpoint.getName(), e);
			closeJmsResources();
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
		return paused.get() ? LifecycleStatus.PAUSED : LifecycleStatus.RUNNING;
	}
}
