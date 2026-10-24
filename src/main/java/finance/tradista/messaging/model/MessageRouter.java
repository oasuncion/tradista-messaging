package finance.tradista.messaging.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
 * Event Router configuring the dispatching of events by event type to subscribing queues (1-to-N).
 * Pure domain model without persistence dependencies.
 * 
 * @author Olivier Asuncion
 */
public class MessageRouter implements Serializable {

	private static final long serialVersionUID = 1L;

	private String name;
	private String description;
	private MessageQueue defaultOutputQueue;
	private Set<RoutingRule> rules = new HashSet<>();

	public MessageRouter() {
	}

	public MessageRouter(String name) {
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

	public MessageQueue getDefaultOutputQueue() {
		return defaultOutputQueue;
	}

	public void setDefaultOutputQueue(MessageQueue defaultOutputQueue) {
		this.defaultOutputQueue = defaultOutputQueue;
	}

	public Set<RoutingRule> getRules() {
		return rules;
	}

	public void setRules(Set<RoutingRule> rules) {
		this.rules = rules != null ? rules : new HashSet<>();
	}

	public void addSubscription(String eventType, MessageQueue destinationQueue) {
		RoutingRule rule = new RoutingRule(this, eventType, destinationQueue);
		this.rules.add(rule);
	}

	/**
	 * Resolves all destination queues subscribed to the given event type (1-to-N).
	 * If no queue is subscribed and a default queue is defined, returns the default queue.
	 *
	 * @param eventType the type of event
	 * @return list of subscribing destination queues
	 */
	public List<MessageQueue> resolveDestinations(String eventType) {
		List<MessageQueue> destinations = new ArrayList<>();
		if (eventType != null) {
			for (RoutingRule rule : rules) {
				if (eventType.equals(rule.getEventType()) && rule.getDestinationQueue() != null) {
					if (!destinations.contains(rule.getDestinationQueue())) {
						destinations.add(rule.getDestinationQueue());
					}
				}
			}
		}
		if (destinations.isEmpty() && defaultOutputQueue != null) {
			destinations.add(defaultOutputQueue);
		}
		return destinations;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof MessageRouter other) {
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
