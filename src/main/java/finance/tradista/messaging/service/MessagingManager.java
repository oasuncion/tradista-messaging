package finance.tradista.messaging.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import finance.tradista.messaging.broker.ConnectionProvider;
import finance.tradista.messaging.broker.EndpointContainer;
import finance.tradista.messaging.broker.EndpointListenerContainer;
import finance.tradista.messaging.broker.EndpointPollerContainer;
import finance.tradista.messaging.broker.MessageConverter;
import finance.tradista.messaging.exception.TradistaMessagingBusinessException;
import finance.tradista.messaging.exception.TradistaMessagingTechnicalException;
import finance.tradista.messaging.model.FilterConfig;
import finance.tradista.messaging.model.LifecycleStatus;
import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageQueue;
import finance.tradista.messaging.model.MessageRouter;
import finance.tradista.messaging.model.RoutingRule;
import finance.tradista.messaging.persistence.entity.FilterConfigEntity;
import finance.tradista.messaging.persistence.entity.MessageEndpointEntity;
import finance.tradista.messaging.persistence.entity.MessageQueueEntity;
import finance.tradista.messaging.persistence.entity.MessageRouterEntity;
import finance.tradista.messaging.persistence.entity.RoutingRuleEntity;
import finance.tradista.messaging.persistence.mapper.FilterConfigMapper;
import finance.tradista.messaging.persistence.mapper.MessageEndpointMapper;
import finance.tradista.messaging.persistence.mapper.MessageQueueMapper;
import finance.tradista.messaging.persistence.mapper.MessageRouterMapper;
import finance.tradista.messaging.persistence.repository.FilterConfigEntityRepository;
import finance.tradista.messaging.persistence.repository.MessageEndpointEntityRepository;
import finance.tradista.messaging.persistence.repository.MessageQueueEntityRepository;
import finance.tradista.messaging.persistence.repository.MessageRouterEntityRepository;
import finance.tradista.messaging.persistence.repository.PersistenceRepositoryFactory;
import finance.tradista.messaging.util.TradistaMessagingUtil;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;

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
 * Core facade and service manager for Tradista Messaging. Operates purely on
 * Domain POJOs while delegating persistence to Jakarta Data repositories.
 * 
 * @author Olivier Asuncion
 */
public final class MessagingManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessagingManager.class);

	private static final Map<String, MessageQueue> RUNTIME_QUEUES = new ConcurrentHashMap<>();
	private static final Map<String, EndpointContainer> BROKER_CONTAINERS = new ConcurrentHashMap<>();

	private static EntityManagerFactory entityManagerFactory;
	private static ConnectionProvider connectionProvider;

	static {
		try {
			entityManagerFactory = Persistence.createEntityManagerFactory("tradista-messaging-persistence-unit");
		} catch (Exception e) {
			LOGGER.error("Failed to initialize EntityManagerFactory: {}", e.getMessage(), e);
		}
	}

	private MessagingManager() {
	}

	public static void setEntityManagerFactory(EntityManagerFactory emf) {
		entityManagerFactory = emf;
	}

	public static void setConnectionProvider(ConnectionProvider provider) {
		connectionProvider = provider;
	}

	public static ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	// -------------------------------------------------------------
	// MessageQueue Services
	// -------------------------------------------------------------

	public static long saveQueue(MessageQueue queue) throws TradistaMessagingBusinessException {
		TradistaMessagingUtil.checkQueue(queue);
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageQueueEntityRepository repo = PersistenceRepositoryFactory.createMessageQueueRepository(session);
			Optional<MessageQueueEntity> existing = repo.findByName(queue.getName());
			if (existing.isPresent()) {
				throw new TradistaMessagingBusinessException(
						String.format("A queue with name '%s' already exists.", queue.getName()));
			}

			Transaction tx = session.beginTransaction();
			try {
				MessageQueueEntity entity = MessageQueueMapper.toEntity(queue);
				entity = repo.insert(entity);
				tx.commit();
				RUNTIME_QUEUES.put(queue.getName(), queue);
				return entity.getId();
			} catch (Exception e) {
				tx.rollback();
				throw e;
			}
		} catch (Exception e) {
			if (e instanceof TradistaMessagingBusinessException tmbe) {
				throw tmbe;
			}
			throw new TradistaMessagingTechnicalException("Error while saving queue: " + e.getMessage(), e);
		}
	}

	public static MessageQueue getQueueByName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return null;
		}
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageQueueEntityRepository repo = PersistenceRepositoryFactory.createMessageQueueRepository(session);
			Optional<MessageQueueEntity> entity = repo.findByName(name);
			if (entity.isPresent()) {
				MessageQueue domain = MessageQueueMapper.toDomain(entity.get());
				RUNTIME_QUEUES.putIfAbsent(name, domain);
				return domain;
			}
			return null;
		}
	}

	public static List<MessageQueue> getAllQueues() {
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageQueueEntityRepository repo = PersistenceRepositoryFactory.createMessageQueueRepository(session);
			List<MessageQueueEntity> entities = repo.findAllOrdered();
			List<MessageQueue> result = new ArrayList<>(entities.size());
			for (MessageQueueEntity entity : entities) {
				result.add(MessageQueueMapper.toDomain(entity));
			}
			return result;
		}
	}

	// -------------------------------------------------------------
	// FilterConfig Services
	// -------------------------------------------------------------

	public static long saveFilterConfig(FilterConfig filterConfig) throws TradistaMessagingBusinessException {
		TradistaMessagingUtil.checkFilterConfig(filterConfig);
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			FilterConfigEntityRepository repo = PersistenceRepositoryFactory.createFilterConfigRepository(session);
			Optional<FilterConfigEntity> existing = repo.findByName(filterConfig.getName());
			if (existing.isPresent()) {
				throw new TradistaMessagingBusinessException(
						String.format("A filter with name '%s' already exists.", filterConfig.getName()));
			}

			Transaction tx = session.beginTransaction();
			try {
				FilterConfigEntity entity = FilterConfigMapper.toEntity(filterConfig);
				entity = repo.insert(entity);
				tx.commit();
				return entity.getId();
			} catch (Exception e) {
				tx.rollback();
				throw e;
			}
		} catch (Exception e) {
			if (e instanceof TradistaMessagingBusinessException tmbe) {
				throw tmbe;
			}
			throw new TradistaMessagingTechnicalException("Error while saving filter config: " + e.getMessage(), e);
		}
	}

	public static FilterConfig getFilterConfigByName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return null;
		}
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			FilterConfigEntityRepository repo = PersistenceRepositoryFactory.createFilterConfigRepository(session);
			return repo.findByName(name).map(FilterConfigMapper::toDomain).orElse(null);
		}
	}

	public static List<FilterConfig> getAllFilterConfigs() {
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			FilterConfigEntityRepository repo = PersistenceRepositoryFactory.createFilterConfigRepository(session);
			List<FilterConfigEntity> entities = repo.findAllOrdered();
			List<FilterConfig> result = new ArrayList<>(entities.size());
			for (FilterConfigEntity entity : entities) {
				result.add(FilterConfigMapper.toDomain(entity));
			}
			return result;
		}
	}

	// -------------------------------------------------------------
	// MessageRouter Services
	// -------------------------------------------------------------

	public static long saveRouter(MessageRouter router) throws TradistaMessagingBusinessException {
		TradistaMessagingUtil.checkRouter(router);
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageRouterEntityRepository repo = PersistenceRepositoryFactory.createMessageRouterRepository(session);
			MessageQueueEntityRepository queueRepo = PersistenceRepositoryFactory.createMessageQueueRepository(session);

			MessageQueueEntity defaultQueueEntity = null;
			if (router.getDefaultOutputQueue() != null) {
				defaultQueueEntity = queueRepo.findByName(router.getDefaultOutputQueue().getName()).orElseThrow(
						() -> new TradistaMessagingBusinessException(String.format("Default queue '%s' does not exist.",
								router.getDefaultOutputQueue().getName())));
			}

			Optional<MessageRouterEntity> existing = repo.findByName(router.getName());

			Transaction tx = session.beginTransaction();
			try {
				MessageRouterEntity entity;
				if (existing.isPresent()) {
					entity = existing.get();
					entity.setDescription(router.getDescription());
					entity.setDefaultOutputQueue(defaultQueueEntity);
					entity = repo.update(entity);
					// Delete existing rules for this router
					session.createMutationQuery("DELETE FROM RoutingRuleEntity r WHERE r.router.id = :routerId")
							.setParameter("routerId", entity.getId()).executeUpdate();
				} else {
					entity = MessageRouterMapper.toEntity(router, defaultQueueEntity);
					entity = repo.insert(entity);
				}

				if (router.getRules() != null) {
					for (RoutingRule rule : router.getRules()) {
						MessageQueueEntity destEntity = queueRepo.findByName(rule.getDestinationQueue().getName())
								.orElseThrow(() -> new TradistaMessagingBusinessException(
										String.format("Destination queue '%s' does not exist.",
												rule.getDestinationQueue().getName())));
						RoutingRuleEntity ruleEntity = new RoutingRuleEntity(entity, rule.getEventType(), destEntity);
						session.insert(ruleEntity);
					}
				}

				tx.commit();
				return entity.getId();
			} catch (Exception e) {
				tx.rollback();
				throw e;
			}
		} catch (Exception e) {
			if (e instanceof TradistaMessagingBusinessException tmbe) {
				throw tmbe;
			}
			throw new TradistaMessagingTechnicalException("Error while saving router: " + e.getMessage(), e);
		}
	}

	public static MessageRouter getRouterByName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return null;
		}
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageRouterEntityRepository repo = PersistenceRepositoryFactory.createMessageRouterRepository(session);
			return repo.findByName(name).map(MessageRouterMapper::toDomain).orElse(null);
		}
	}

	public static List<MessageRouter> getAllRouters() {
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageRouterEntityRepository repo = PersistenceRepositoryFactory.createMessageRouterRepository(session);
			List<MessageRouterEntity> entities = repo.findAllOrdered();
			List<MessageRouter> result = new ArrayList<>(entities.size());
			for (MessageRouterEntity entity : entities) {
				result.add(MessageRouterMapper.toDomain(entity));
			}
			return result;
		}
	}

	// -------------------------------------------------------------
	// MessageEndpoint & Lifecycle Services
	// -------------------------------------------------------------

	public static long saveEndpoint(MessageEndpoint endpoint) throws TradistaMessagingBusinessException {
		TradistaMessagingUtil.checkEndpoint(endpoint);
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageEndpointEntityRepository repo = PersistenceRepositoryFactory
					.createMessageEndpointRepository(session);
			MessageQueueEntityRepository queueRepo = PersistenceRepositoryFactory.createMessageQueueRepository(session);
			FilterConfigEntityRepository filterRepo = PersistenceRepositoryFactory
					.createFilterConfigRepository(session);

			MessageQueueEntity queueEntity = queueRepo.findByName(endpoint.getQueue().getName())
					.orElseThrow(() -> new TradistaMessagingBusinessException(
							String.format("Queue '%s' does not exist.", endpoint.getQueue().getName())));

			FilterConfigEntity filterConfigEntity = null;
			if (endpoint.getFilterConfig() != null) {
				filterConfigEntity = filterRepo.findByName(endpoint.getFilterConfig().getName())
						.orElseThrow(() -> new TradistaMessagingBusinessException(
								String.format("Filter '%s' does not exist.", endpoint.getFilterConfig().getName())));
			}

			Optional<MessageEndpointEntity> existing = repo.findByName(endpoint.getName());

			Transaction tx = session.beginTransaction();
			try {
				MessageEndpointEntity entity;
				if (existing.isPresent()) {
					entity = existing.get();
					entity.setDescription(endpoint.getDescription());
					entity.setQueue(queueEntity);
					entity.setFilterConfig(filterConfigEntity);
					entity.setConsumerClass(endpoint.getConsumerClass());
					if (endpoint.getPollerConfig() != null) {
						entity.getPollerConfig().setDelayMs(endpoint.getPollerConfig().getDelayMs());
						entity.getPollerConfig()
								.setMaxMessagesPerPoll(endpoint.getPollerConfig().getMaxMessagesPerPoll());
					}
					entity.setStatus(endpoint.getStatus());
					entity = repo.update(entity);
				} else {
					entity = MessageEndpointMapper.toEntity(endpoint, queueEntity, filterConfigEntity);
					entity = repo.insert(entity);
				}
				tx.commit();
				return entity.getId();
			} catch (Exception e) {
				tx.rollback();
				throw e;
			}
		} catch (Exception e) {
			if (e instanceof TradistaMessagingBusinessException tmbe) {
				throw tmbe;
			}
			throw new TradistaMessagingTechnicalException("Error while saving endpoint: " + e.getMessage(), e);
		}
	}

	public static MessageEndpoint getEndpointByName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return null;
		}
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageEndpointEntityRepository repo = PersistenceRepositoryFactory
					.createMessageEndpointRepository(session);
			return repo.findByName(name).map(MessageEndpointMapper::toDomain).orElse(null);
		}
	}

	public static List<MessageEndpoint> getAllEndpoints() {
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageEndpointEntityRepository repo = PersistenceRepositoryFactory
					.createMessageEndpointRepository(session);
			List<MessageEndpointEntity> entities = repo.findAllOrdered();
			List<MessageEndpoint> result = new ArrayList<>(entities.size());
			for (MessageEndpointEntity entity : entities) {
				result.add(MessageEndpointMapper.toDomain(entity));
			}
			return result;
		}
	}

	public static void startEndpoint(String name) throws TradistaMessagingBusinessException {
		updateEndpointStatus(name, LifecycleStatus.RUNNING);
	}

	public static void stopEndpoint(String name) throws TradistaMessagingBusinessException {
		updateEndpointStatus(name, LifecycleStatus.STOPPED);
	}

	public static void pauseEndpoint(String name) throws TradistaMessagingBusinessException {
		updateEndpointStatus(name, LifecycleStatus.PAUSED);
	}

	private static void updateEndpointStatus(String name, LifecycleStatus status)
			throws TradistaMessagingBusinessException {
		MessageEndpoint endpoint = getEndpointByName(name);
		if (endpoint == null) {
			throw new TradistaMessagingBusinessException(String.format("Endpoint '%s' does not exist.", name));
		}

		endpoint.setStatus(status);
		try (StatelessSession session = PersistenceRepositoryFactory.openStatelessSession(entityManagerFactory)) {
			MessageEndpointEntityRepository repo = PersistenceRepositoryFactory
					.createMessageEndpointRepository(session);
			Optional<MessageEndpointEntity> existing = repo.findByName(name);
			if (existing.isPresent()) {
				Transaction tx = session.beginTransaction();
				try {
					MessageEndpointEntity entity = existing.get();
					entity.setStatus(status);
					repo.update(entity);
					tx.commit();
				} catch (Exception e) {
					tx.rollback();
					throw e;
				}
			}
		}

		final MessageEndpoint finalEndpoint = endpoint;
		if (connectionProvider != null) {
			try {
				EndpointContainer container = BROKER_CONTAINERS.computeIfAbsent(name, _ -> {
					if (finalEndpoint.getPollerConfig() != null) {
						return new EndpointPollerContainer(finalEndpoint, connectionProvider);
					}
					return new EndpointListenerContainer(finalEndpoint, connectionProvider);
				});
				switch (status) {
				case RUNNING -> container.start();
				case PAUSED -> container.pause();
				case STOPPED -> container.stop();
				}
			} catch (Exception e) {
				LOGGER.error("Failed to update container status for endpoint {}: {}", name, e.getMessage(), e);
				throw new TradistaMessagingTechnicalException("Failed to update container status for endpoint: " + name,
						e);
			}
		}

		LOGGER.info("Endpoint {} updated to status {}", name, status);
	}

	// -------------------------------------------------------------
	// Messaging Operations (Send, Receive, Route, Poller)
	// -------------------------------------------------------------

	public static <T extends Serializable> void send(String queueName, Message<T> message) {
		if (connectionProvider != null) {
			try {
				Connection connection = connectionProvider.getConnection();
				try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
					Destination destination = connectionProvider.resolveQueue(session, queueName);
					try (MessageProducer producer = session.createProducer(destination)) {
						jakarta.jms.Message brokerMessage = MessageConverter.toBrokerMessage(session, message, null);
						producer.send(brokerMessage);
					}
				}
				return;
			} catch (JMSException e) {
				throw new TradistaMessagingTechnicalException("Send failed on queue: " + queueName, e);
			}
		}

		MessageQueue queue = RUNTIME_QUEUES.computeIfAbsent(queueName, q -> {
			MessageQueue fetched = getQueueByName(q);
			return fetched != null ? fetched : new MessageQueue(q);
		});

		try {
			queue.getRuntimeQueue().put(message);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TradistaMessagingTechnicalException("Thread interrupted while sending to queue " + queueName, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> Message<T> receive(String queueName) {
		MessageQueue queue = RUNTIME_QUEUES.get(queueName);
		if (queue == null) {
			queue = getQueueByName(queueName);
		}
		if (queue == null) {
			return null;
		}
		return (Message<T>) queue.getRuntimeQueue().poll();
	}

	private static <T extends Serializable> void route(MessageRouter router, Message<T> message, String eventType) {
		if (router == null || message == null) {
			return;
		}
		List<MessageQueue> destinations = router.resolveDestinations(eventType);
		for (MessageQueue dest : destinations) {
			send(dest.getName(), message);
		}
	}

	public static <T extends Serializable> void publishEvent(String routerName, String eventType, Message<T> message)
			throws TradistaMessagingBusinessException {
		MessageRouter router = getRouterByName(routerName);
		if (router == null) {
			throw new TradistaMessagingBusinessException(String.format("Router '%s' does not exist.", routerName));
		}
		route(router, message, eventType);
	}
}
