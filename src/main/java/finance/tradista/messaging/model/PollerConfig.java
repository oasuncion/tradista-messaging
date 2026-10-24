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
 * Configuration for poller endpoints.
 * Pure domain model without persistence dependencies.
 * 
 * @author Olivier Asuncion
 */
public class PollerConfig implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private long delayMs = 1000L;
	private int maxMessagesPerPoll = 10;

	public PollerConfig() {
	}

	public PollerConfig(long delayMs, int maxMessagesPerPoll) {
		this.delayMs = delayMs;
		this.maxMessagesPerPoll = maxMessagesPerPoll;
	}

	public long getDelayMs() {
		return delayMs;
	}

	public void setDelayMs(long delayMs) {
		this.delayMs = delayMs;
	}

	public int getMaxMessagesPerPoll() {
		return maxMessagesPerPoll;
	}

	public void setMaxMessagesPerPoll(int maxMessagesPerPoll) {
		this.maxMessagesPerPoll = maxMessagesPerPoll;
	}

	@Override
	public PollerConfig clone() {
		try {
			return (PollerConfig) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		PollerConfig other = (PollerConfig) obj;
		return delayMs == other.delayMs && maxMessagesPerPoll == other.maxMessagesPerPoll;
	}

	@Override
	public int hashCode() {
		return Objects.hash(delayMs, maxMessagesPerPoll);
	}

	@Override
	public String toString() {
		return "PollerConfig [delayMs=" + delayMs + ", maxMessagesPerPoll=" + maxMessagesPerPoll + "]";
	}
}
