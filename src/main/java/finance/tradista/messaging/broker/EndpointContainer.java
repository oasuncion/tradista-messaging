package finance.tradista.messaging.broker;

import finance.tradista.messaging.model.LifecycleStatus;
import jakarta.jms.JMSException;

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
 * Common lifecycle interface for live messaging endpoint containers (listeners
 * and pollers).
 * 
 * @author Olivier Asuncion
 */
public interface EndpointContainer extends AutoCloseable {

	/**
	 * Starts or resumes message consumption.
	 * 
	 * @throws JMSException if connection or session creation fails
	 */
	void start() throws JMSException;

	/**
	 * Temporarily suspends message consumption without closing underlying
	 * resources.
	 */
	void pause();

	/**
	 * Stops message consumption and releases underlying resources.
	 */
	void stop();

	/**
	 * Checks if the container is currently running and active.
	 * 
	 * @return true if running and not paused, false otherwise
	 */
	boolean isRunning();

	/**
	 * Checks if the container is currently paused.
	 * 
	 * @return true if paused, false otherwise
	 */
	boolean isPaused();

	/**
	 * Returns the current lifecycle status of the container.
	 * 
	 * @return LifecycleStatus RUNNING, PAUSED, or STOPPED
	 */
	LifecycleStatus getStatus();

	@Override
	void close();
}
