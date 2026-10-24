package finance.tradista.messaging.persistence.entity;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
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
 * JPA Entity for MessageRouter persistence.
 * 
 * @author Olivier Asuncion
 */
@Entity
@Table(name = "MESSAGING_ROUTER")
public class MessageRouterEntity extends BaseMessagingEntity {

	private static final long serialVersionUID = 1L;

	@Column(name = "NAME", unique = true, nullable = false, length = 100)
	private String name;

	@Column(name = "DESCRIPTION", length = 255)
	private String description;

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "DEFAULT_OUTPUT_QUEUE_ID")
	private MessageQueueEntity defaultOutputQueue;

	@OneToMany(mappedBy = "router", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
	private Set<RoutingRuleEntity> rules = new HashSet<>();

	public MessageRouterEntity() {
	}

	public MessageRouterEntity(String name) {
		this.name = name;
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

	public MessageQueueEntity getDefaultOutputQueue() {
		return defaultOutputQueue;
	}

	public void setDefaultOutputQueue(MessageQueueEntity defaultOutputQueue) {
		this.defaultOutputQueue = defaultOutputQueue;
	}

	public Set<RoutingRuleEntity> getRules() {
		return rules;
	}

	public void setRules(Set<RoutingRuleEntity> rules) {
		this.rules = rules != null ? rules : new HashSet<>();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof MessageRouterEntity other) {
			return Objects.equals(name, other.name);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}
}
