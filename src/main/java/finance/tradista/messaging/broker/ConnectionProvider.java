package finance.tradista.messaging.broker;

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
 * Provider interface supplying Jakarta Messaging Connections and resolving
 * Destinations.
 * 
 * @author Olivier Asuncion
 */
public interface ConnectionProvider extends AutoCloseable {

	/**
	 * Returns the underlying Jakarta Messaging ConnectionFactory.
	 * 
	 * @return ConnectionFactory
	 */
	ConnectionFactory getConnectionFactory();

	/**
	 * Returns a managed or shared Jakarta Messaging Connection.
	 * 
	 * @return active Connection
	 * @throws JMSException in case of error
	 */
	Connection getConnection() throws JMSException;

	/**
	 * Resolves a Queue Destination from its name.
	 * 
	 * @param session   Jakarta Messaging session
	 * @param queueName Queue name
	 * @return Destination Queue
	 * @throws JMSException in case of error
	 */
	Destination resolveQueue(Session session, String queueName) throws JMSException;
}
