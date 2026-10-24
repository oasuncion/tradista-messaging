package finance.tradista.messaging.persistence.entity;

import java.util.Objects;

import finance.tradista.messaging.model.LifecycleStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

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
 * JPA Entity for MessageEndpoint persistence.
 * 
 * @author Olivier Asuncion
 */
@Entity
@Table(name = "MESSAGING_ENDPOINT")
public class MessageEndpointEntity extends BaseMessagingEntity {

	private static final long serialVersionUID = 1L;

	@Column(name = "NAME", unique = true, nullable = false, length = 100)
	private String name;

	@Column(name = "DESCRIPTION", length = 255)
	private String description;

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "QUEUE_ID", nullable = false)
	private MessageQueueEntity queue;

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "FILTER_CONFIG_ID")
	private FilterConfigEntity filterConfig;

	@Column(name = "CONSUMER_CLASS", length = 255)
	private String consumerClass;

	@Embedded
	private PollerConfigEmbeddable pollerConfig = new PollerConfigEmbeddable();

	@Enumerated(EnumType.STRING)
	@Column(name = "STATUS", length = 20, nullable = false)
	private LifecycleStatus status = LifecycleStatus.STOPPED;

	public MessageEndpointEntity() {
	}

	public MessageEndpointEntity(String name, MessageQueueEntity queue) {
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

	public MessageQueueEntity getQueue() {
		return queue;
	}

	public void setQueue(MessageQueueEntity queue) {
		this.queue = queue;
	}

	public FilterConfigEntity getFilterConfig() {
		return filterConfig;
	}

	public void setFilterConfig(FilterConfigEntity filterConfig) {
		this.filterConfig = filterConfig;
	}

	public String getConsumerClass() {
		return consumerClass;
	}

	public void setConsumerClass(String consumerClass) {
		this.consumerClass = consumerClass;
	}

	public PollerConfigEmbeddable getPollerConfig() {
		return pollerConfig;
	}

	public void setPollerConfig(PollerConfigEmbeddable pollerConfig) {
		this.pollerConfig = pollerConfig;
	}

	public LifecycleStatus getStatus() {
		return status;
	}

	public void setStatus(LifecycleStatus status) {
		this.status = status;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof MessageEndpointEntity other) {
			return Objects.equals(name, other.name);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}
}
