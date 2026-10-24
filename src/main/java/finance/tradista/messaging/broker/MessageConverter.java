package finance.tradista.messaging.broker;

import java.io.Serializable;
import java.util.Map;

import finance.tradista.messaging.model.Message;
import jakarta.jms.JMSException;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

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
 * Converter between Tradista Message and Jakarta Messaging Message.
 * 
 * @author Olivier Asuncion
 */
public final class MessageConverter {

	public static final String PROPERTY_EVENT_TYPE = "TradistaEventType";
	public static final String PROPERTY_MESSAGE_ID = "TradistaMessageId";

	private MessageConverter() {
	}

	public static <T extends Serializable> jakarta.jms.Message toBrokerMessage(Session session, Message<T> message, String eventType)
			throws JMSException {
		jakarta.jms.Message brokerMessage;
		T payload = message.getPayload();
		if (payload instanceof String str) {
			brokerMessage = session.createTextMessage(str);
		} else if (payload instanceof Serializable ser) {
			brokerMessage = session.createObjectMessage(ser);
		} else {
			brokerMessage = session.createTextMessage(payload != null ? payload.toString() : null);
		}

		if (eventType != null) {
			brokerMessage.setStringProperty(PROPERTY_EVENT_TYPE, eventType);
		}
		if (message.getId() != null) {
			brokerMessage.setStringProperty(PROPERTY_MESSAGE_ID, message.getId());
		}

		if (message.getHeaders() != null) {
			for (Map.Entry<String, Serializable> entry : message.getHeaders().entrySet()) {
				brokerMessage.setObjectProperty(entry.getKey(), entry.getValue());
			}
		}

		return brokerMessage;
	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> Message<T> fromBrokerMessage(jakarta.jms.Message brokerMessage) throws JMSException {
		if (brokerMessage == null) {
			return null;
		}

		T payload = null;
		if (brokerMessage instanceof TextMessage textMsg) {
			payload = (T) textMsg.getText();
		} else if (brokerMessage instanceof ObjectMessage objMsg) {
			payload = (T) objMsg.getObject();
		}

		String messageId = brokerMessage.propertyExists(PROPERTY_MESSAGE_ID)
				? brokerMessage.getStringProperty(PROPERTY_MESSAGE_ID)
				: null;

		java.util.Map<String, Serializable> headers = new java.util.HashMap<>();
		var propertyNames = brokerMessage.getPropertyNames();
		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			if (!PROPERTY_MESSAGE_ID.equals(name)) {
				Object prop = brokerMessage.getObjectProperty(name);
				if (prop instanceof Serializable ser) {
					headers.put(name, ser);
				}
			}
		}

		return new Message<>(messageId, java.time.Instant.now(), payload, headers);
	}
}
