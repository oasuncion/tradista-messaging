package finance.tradista.messaging.model;

import java.io.Serializable;
import java.util.Objects;

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
 * Routing rule linking an event type to a subscribing destination queue.
 * 
 * @author Olivier Asuncion
 */
public class RoutingRule implements Serializable {

	private static final long serialVersionUID = 1L;

	private MessageRouter router;
	private String eventType;
	private MessageQueue destinationQueue;

	public RoutingRule() {
	}

	public RoutingRule(MessageRouter router, String eventType, MessageQueue destinationQueue) {
		this.router = router;
		this.eventType = eventType;
		this.destinationQueue = destinationQueue;
	}

	public MessageRouter getRouter() {
		return router;
	}

	public void setRouter(MessageRouter router) {
		this.router = router;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public MessageQueue getDestinationQueue() {
		return destinationQueue;
	}

	public void setDestinationQueue(MessageQueue destinationQueue) {
		this.destinationQueue = destinationQueue;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		RoutingRule other = (RoutingRule) obj;
		return Objects.equals(eventType, other.eventType) && Objects.equals(destinationQueue, other.destinationQueue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventType, destinationQueue);
	}

	@Override
	public String toString() {
		return eventType + " -> " + (destinationQueue != null ? destinationQueue.getName() : "null");
	}
}
