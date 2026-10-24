# Tradista Messaging

![License](https://img.shields.io/badge/License-Apache_2.0-33ff99.svg?link=https://www.apache.org/licenses/LICENSE-2.0)&emsp;
![Version](https://img.shields.io/badge/Version%20-%201.0.0--SNAPSHOT%20-%2033ff99?color=33ff99)

**Tradista Messaging** is a lightweight, standalone Java messaging and event routing framework that can be integrated into any Java SE or Jakarta EE application.

It provides simple, declarative and programmatic ways to configure and persist:
- **Message Queues & Channels** (Point-to-Point, Publish-Subscribe)
- **Event-based Routing** (Dispatching messages to dedicated queues according to event types)
- **Message Filters** (Accepting or rejecting messages based on predicates)
- **Message Endpoints & Lifecycle** (Poller intervals, max messages per poll, Start / Stop / Pause states)


---

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
  <groupId>finance.tradista.messaging</groupId>
  <artifactId>tradista-messaging</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## How to Use Tradista Messaging

The framework is operated via the static facade `MessagingManager`. It can operate both directly over an actual **Jakarta Messaging** broker (e.g., ActiveMQ Artemis, WildFly Messaging, etc.) and via in-memory queues.

### 1. Connecting to a Real Jakarta Messaging Broker

Supply any standard `jakarta.jms.ConnectionFactory` to Tradista Messaging:

```java
// Example with ActiveMQ Artemis or any Jakarta Messaging ConnectionFactory
ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
MessagingManager.setConnectionProvider(new DefaultConnectionProvider(cf));
```

---

### 2. Configure and Persist Queues

Create and persist queues with optional capacity constraints. `MessageQueue` directly implements **`jakarta.jms.Queue`**, making it natively usable across standard Jakarta Messaging APIs:

```java
// Create a MessageQueue (which implements jakarta.jms.Queue)
MessageQueue tradeQueue = new MessageQueue("TRADE_QUEUE");
tradeQueue.setDescription("Queue for incoming trade events");
tradeQueue.setCapacity(500);

MessagingManager.saveQueue(tradeQueue);

// Retrieve a queue by name
MessageQueue fetched = MessagingManager.getQueueByName("TRADE_QUEUE");
String queueName = fetched.getQueueName(); // "TRADE_QUEUE"
```

---

### 2. Configure and Persist Message Filters

Implement the functional interface `MessageFilter<T>`:

```java
package com.mycompany.filters;

import finance.tradista.messaging.model.MessageFilter;

public class TradePrefixFilter implements MessageFilter<String> {
    @Override
    public boolean accept(String message) {
        return message != null && message.startsWith("EXPORT_");
    }
}
```

Persist the filter configuration so it can be associated with any endpoint:

```java
FilterConfig filterConfig = new FilterConfig("PREFIX_FILTER", "com.mycompany.filters.TradePrefixFilter");
filterConfig.setDescription("Filter accepting only EXPORT_ messages");

MessagingManager.saveFilterConfig(filterConfig);
```

---

### 3. Configure and Persist an Event Router (Event Publication & Subscriptions)

Define routing rules where producers publish an event of a specific `eventType` (e.g. `"TRADE"`, `"CASH_TRANSFER"`), and the router automatically dispatches it to all subscribed queues (1-to-N):

```java
// Define destination queues for consumers
MessageQueue fixQueue = new MessageQueue("FIX_EXPORT_QUEUE");
MessagingManager.saveQueue(fixQueue);

MessageQueue transferQueue = new MessageQueue("TRANSFER_QUEUE");
MessagingManager.saveQueue(transferQueue);

MessageQueue defaultQueue = new MessageQueue("DEFAULT_QUEUE");
MessagingManager.saveQueue(defaultQueue);

// Configure router with event subscriptions and optional default fallback
MessageRouter router = new MessageRouter("TRADE_EVENT_ROUTER");
router.setDefaultOutputQueue(defaultQueue);

// Subscribe queues to the REPO_TRADE event type
router.addSubscription("REPO_TRADE", fixQueue);
router.addSubscription("REPO_TRADE", transferQueue);

MessagingManager.saveRouter(router);

// Publish an event: dispatches automatically to both FIX_EXPORT_QUEUE and TRANSFER_QUEUE
Message<String> tradeEvent = new Message<>("REPO_TRADE_001");
MessagingManager.publishEvent("TRADE_EVENT_ROUTER", "REPO_TRADE", tradeEvent);
```

---

### 4. Configure Message Endpoints, Polling, and Lifecycle

Configure a consumer endpoint with a poller schedule and filter:

```java
// Consumer implementation
public class FixExportConsumer implements MessageConsumer<String> {
    @Override
    public void onMessage(Message<String> message) {
        System.out.println("Processing trade: " + message.getPayload());
    }
}

// Endpoint definition
MessageEndpoint endpoint = new MessageEndpoint("FIX_EXPORT_ENDPOINT", fixQueue);
endpoint.setFilterConfig(filterConfig);
endpoint.setConsumerClass(FixExportConsumer.class.getName());
endpoint.setPollerConfig(new PollerConfig(500L /* delay ms */, 10 /* max messages per poll */));

// Save endpoint (initial state is STOPPED)
MessagingManager.saveEndpoint(endpoint);

// Control lifecycle
MessagingManager.startEndpoint("FIX_EXPORT_ENDPOINT"); // LifecycleStatus.RUNNING
// MessagingManager.pauseEndpoint("FIX_EXPORT_ENDPOINT"); // LifecycleStatus.PAUSED
// MessagingManager.stopEndpoint("FIX_EXPORT_ENDPOINT");  // LifecycleStatus.STOPPED

// Trigger polling cycle
int processed = MessagingManager.pollEndpoint("FIX_EXPORT_ENDPOINT");
```

---

## Persistence Configuration (JPA)

Tradista Messaging relies on JPA (Jakarta Persistence 3.x) and supports both `RESOURCE_LOCAL` and `JTA` environments.

Place a `persistence.xml` file under `META-INF/persistence.xml` in your classpath:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<persistence xmlns="https://jakarta.ee/xml/ns/persistence"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="3.0">
	<persistence-unit
		name="tradista-messaging-persistence-unit"
		transaction-type="RESOURCE_LOCAL">
		<description>Persistence unit for Tradista Messaging.</description>
		<class>finance.tradista.messaging.persistence.entity.BaseMessagingEntity</class>
		<class>finance.tradista.messaging.persistence.entity.MessageQueueEntity</class>
		<class>finance.tradista.messaging.persistence.entity.FilterConfigEntity</class>
		<class>finance.tradista.messaging.persistence.entity.MessageRouterEntity</class>
		<class>finance.tradista.messaging.persistence.entity.RoutingRuleEntity</class>
		<class>finance.tradista.messaging.persistence.entity.MessageEndpointEntity</class>
		<properties>
			<property name="jakarta.persistence.jdbc.driver" value="org.h2.Driver" />
			<property name="jakarta.persistence.jdbc.url" value="jdbc:h2:mem:tradistamessaging;DB_CLOSE_DELAY=-1" />
			<property name="jakarta.persistence.jdbc.user" value="sa" />
			<property name="jakarta.persistence.jdbc.password" value="" />
			<property name="hibernate.show_sql" value="false" />
			<property name="jakarta.persistence.schema-generation.database.action" value="drop-and-create" />
		</properties>
	</persistence-unit>
</persistence>
```

---

## Dependencies

Tradista Messaging is built on modern Jakarta EE standards:
- [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/) for enterprise message brokering (Queue destinations, consumers, producers)
- [Jakarta Data](https://jakarta.ee/specifications/data/) for declarative repositories
- [Jakarta Query](https://jakarta.ee/specifications/query/) for standardized query language definitions (`@Query`)
- [Jakarta Persistence (JPA)](https://jakarta.ee/specifications/persistence/) for relational mapping
- [Hibernate ORM](https://hibernate.org/orm/) as the persistence provider
- [H2 Database](https://www.h2database.com/) for fast in-memory testing
- [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) for embedded JMS broker integration testing
- [Apache Log4j 2](https://logging.apache.org/log4j/2.x/) for logging
- [JUnit 5](https://junit.org/junit5/) for unit testing
- [JReleaser](https://jreleaser.org/) for automated releases to Maven Central
