package finance.tradista.messaging.jms;

import java.util.Objects;

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
 * Standard implementation of JmsConnectionProvider backed by any Jakarta JMS ConnectionFactory.
 * 
 * @author Olivier Asuncion
 */
public class DefaultJmsConnectionProvider implements JmsConnectionProvider {

	private final ConnectionFactory connectionFactory;
	private Connection connection;

	public DefaultJmsConnectionProvider(ConnectionFactory connectionFactory) {
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

	public synchronized void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				// Ignore on close
			}
			connection = null;
		}
	}
}
