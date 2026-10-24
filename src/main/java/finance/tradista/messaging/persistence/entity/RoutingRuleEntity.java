package finance.tradista.messaging.persistence.entity;

import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
 * JPA Entity for RoutingRule persistence.
 * 
 * @author Olivier Asuncion
 */
@Entity
@Table(name = "MESSAGING_ROUTING_RULE")
public class RoutingRuleEntity extends BaseMessagingEntity {

	private static final long serialVersionUID = 1L;

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "ROUTER_ID", nullable = false)
	private MessageRouterEntity router;

	@Column(name = "EVENT_TYPE", nullable = false, length = 100)
	private String eventType;

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "DESTINATION_QUEUE_ID", nullable = false)
	private MessageQueueEntity destinationQueue;

	public RoutingRuleEntity() {
	}

	public RoutingRuleEntity(MessageRouterEntity router, String eventType, MessageQueueEntity destinationQueue) {
		this.router = router;
		this.eventType = eventType;
		this.destinationQueue = destinationQueue;
	}

	public MessageRouterEntity getRouter() {
		return router;
	}

	public void setRouter(MessageRouterEntity router) {
		this.router = router;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public MessageQueueEntity getDestinationQueue() {
		return destinationQueue;
	}

	public void setDestinationQueue(MessageQueueEntity destinationQueue) {
		this.destinationQueue = destinationQueue;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		RoutingRuleEntity other = (RoutingRuleEntity) obj;
		return Objects.equals(eventType, other.eventType) && Objects.equals(destinationQueue, other.destinationQueue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventType, destinationQueue);
	}
}
