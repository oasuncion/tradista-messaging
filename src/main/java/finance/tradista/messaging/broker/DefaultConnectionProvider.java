package finance.tradista.messaging.broker;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import finance.tradista.messaging.exception.TradistaMessagingTechnicalException;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
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
 * Standard implementation of ConnectionProvider backed by any Jakarta Messaging ConnectionFactory.
 * 
 * @author Olivier Asuncion
 */
public class DefaultConnectionProvider implements ConnectionProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConnectionProvider.class);

	private final ConnectionFactory connectionFactory;
	private Connection connection;

	public DefaultConnectionProvider(ConnectionFactory connectionFactory) {
		this.connectionFactory = Objects.requireNonNull(connectionFactory, "ConnectionFactory cannot be null");
	}

	@Override
	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	@Override
	public synchronized Connection getConnection() throws JMSException {
		if (connection == null) {
			connection = connectionFactory.createConnection();
			connection.start();
		}
		return connection;
	}

	@Override
	public Destination resolveQueue(Session session, String queueName) throws JMSException {
		return session.createQueue(queueName);
	}

	@Override
	public synchronized void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				LOGGER.error("Failed to close Jakarta Messaging connection: {}", e.getMessage(), e);
				throw new TradistaMessagingTechnicalException("Failed to close connection: " + e.getMessage(), e);
			} finally {
				connection = null;
			}
		}
	}
}
