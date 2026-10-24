package finance.tradista.messaging.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import jakarta.jms.Queue;

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
 * Message Queue representing a point-to-point destination.
 * Implements jakarta.jms.Queue to natively integrate with Jakarta Messaging.
 * Pure domain model without persistence dependencies.
 * 
 * @author Olivier Asuncion
 */
public class MessageQueue implements Queue, Serializable {

	private static final long serialVersionUID = 1L;

	private String name;
	private String description;
	private Integer capacity;

	private transient BlockingQueue<Message<?>> runtimeQueue;

	public MessageQueue() {
		initRuntimeQueue();
	}

	public MessageQueue(String name) {
		this();
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getQueueName() {
		return getName();
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getCapacity() {
		return capacity;
	}

	public void setCapacity(Integer capacity) {
		this.capacity = capacity;
		initRuntimeQueue();
	}

	private void initRuntimeQueue() {
		if (capacity != null && capacity > 0) {
			runtimeQueue = new LinkedBlockingQueue<>(capacity);
		} else {
			runtimeQueue = new LinkedBlockingQueue<>();
		}
	}

	public BlockingQueue<Message<?>> getRuntimeQueue() {
		if (runtimeQueue == null) {
			initRuntimeQueue();
		}
		return runtimeQueue;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof MessageQueue other) {
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
