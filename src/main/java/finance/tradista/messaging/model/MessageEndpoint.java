package finance.tradista.messaging.model;

import java.io.Serializable;
import java.util.Objects;

import finance.tradista.messaging.exception.TradistaMessagingTechnicalException;

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
 * Message Endpoint representing a consumer/listener connected to a queue, with
 * optional filter and poller configuration, managing its lifecycle.
 * Pure domain model without persistence dependencies.
 * 
 * @author Olivier Asuncion
 */
public class MessageEndpoint implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private String name;
	private String description;
	private MessageQueue queue;
	private FilterConfig filterConfig;
	private String consumerClass;
	private PollerConfig pollerConfig = new PollerConfig();
	private LifecycleStatus status = LifecycleStatus.STOPPED;

	private transient MessageConsumer<?> consumerInstance;

	public MessageEndpoint() {
	}

	public MessageEndpoint(String name, MessageQueue queue) {
		this.name = name;
		this.queue = queue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public MessageQueue getQueue() {
		return queue;
	}

	public void setQueue(MessageQueue queue) {
		this.queue = queue;
	}

	public FilterConfig getFilterConfig() {
		return filterConfig;
	}

	public void setFilterConfig(FilterConfig filterConfig) {
		this.filterConfig = filterConfig;
	}

	public String getConsumerClass() {
		return consumerClass;
	}

	public void setConsumerClass(String consumerClass) {
		this.consumerClass = consumerClass;
		this.consumerInstance = null;
	}

	public PollerConfig getPollerConfig() {
		return pollerConfig;
	}

	public void setPollerConfig(PollerConfig pollerConfig) {
		this.pollerConfig = pollerConfig;
	}

	public LifecycleStatus getStatus() {
		return status;
	}

	public void setStatus(LifecycleStatus status) {
		this.status = status;
	}

	@SuppressWarnings("unchecked")
	public <T> MessageConsumer<T> getConsumerInstance() {
		if (consumerInstance == null && consumerClass != null) {
			try {
				Class<?> clazz = Class.forName(consumerClass);
				consumerInstance = (MessageConsumer<?>) clazz.getDeclaredConstructor().newInstance();
			} catch (Exception e) {
				throw new TradistaMessagingTechnicalException("Cannot instantiate consumer class " + consumerClass, e);
			}
		}
		return (MessageConsumer<T>) consumerInstance;
	}

	public void setConsumerInstance(MessageConsumer<?> consumerInstance) {
		this.consumerInstance = consumerInstance;
		if (consumerInstance != null) {
			this.consumerClass = consumerInstance.getClass().getName();
		}
	}

	@Override
	public MessageEndpoint clone() {
		try {
			MessageEndpoint clone = (MessageEndpoint) super.clone();
			if (pollerConfig != null) {
				clone.pollerConfig = pollerConfig.clone();
			}
			clone.consumerInstance = null;
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof MessageEndpoint other) {
			return Objects.equals(name, other.name);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return name;
	}
}
