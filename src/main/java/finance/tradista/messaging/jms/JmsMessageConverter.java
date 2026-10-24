package finance.tradista.messaging.jms;

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
 * Converter between Tradista Message and Jakarta JMS Message.
 * 
 * @author Olivier Asuncion
 */
public final class JmsMessageConverter {

	public static final String PROPERTY_EVENT_TYPE = "TradistaEventType";
	public static final String PROPERTY_MESSAGE_ID = "TradistaMessageId";

	private JmsMessageConverter() {
	}

	public static <T> jakarta.jms.Message toJmsMessage(Session session, Message<T> message, String eventType)
			throws JMSException {
		jakarta.jms.Message jmsMsg;
		T payload = message.getPayload();
		if (payload instanceof String str) {
			jmsMsg = session.createTextMessage(str);
		} else if (payload instanceof Serializable ser) {
			jmsMsg = session.createObjectMessage(ser);
		} else {
			jmsMsg = session.createTextMessage(payload != null ? payload.toString() : null);
		}

		jmsMsg.setStringProperty(PROPERTY_MESSAGE_ID, message.getId());
		if (eventType != null) {
			jmsMsg.setStringProperty(PROPERTY_EVENT_TYPE, eventType);
		}

		for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
			if (entry.getValue() != null) {
				jmsMsg.setObjectProperty(entry.getKey(), entry.getValue());
			}
		}

		return jmsMsg;
	}

	@SuppressWarnings("unchecked")
	public static <T> Message<T> fromJmsMessage(jakarta.jms.Message jmsMsg) throws JMSException {
		T payload = null;
		if (jmsMsg instanceof TextMessage txt) {
			payload = (T) txt.getText();
		} else if (jmsMsg instanceof ObjectMessage obj) {
			payload = (T) obj.getObject();
		}

		Message<T> msg = new Message<>(payload);

		var propertyNames = jmsMsg.getPropertyNames();
		while (propertyNames.hasMoreElements()) {
			String prop = (String) propertyNames.nextElement();
			msg.setHeader(prop, jmsMsg.getObjectProperty(prop));
		}

		return msg;
	}
}
