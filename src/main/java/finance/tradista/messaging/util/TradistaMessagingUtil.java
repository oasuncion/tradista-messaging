package finance.tradista.messaging.util;

import finance.tradista.messaging.exception.TradistaMessagingBusinessException;
import finance.tradista.messaging.model.FilterConfig;
import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.MessageQueue;
import finance.tradista.messaging.model.MessageRouter;
import finance.tradista.messaging.model.RoutingRule;

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
 * Utility class providing validation for messaging objects.
 * 
 * @author Olivier Asuncion
 */
public final class TradistaMessagingUtil {

	private TradistaMessagingUtil() {
	}

	public static void checkQueue(MessageQueue queue) throws TradistaMessagingBusinessException {
		if (queue == null) {
			throw new TradistaMessagingBusinessException("Queue cannot be null.");
		}
		if (queue.getName() == null || queue.getName().trim().isEmpty()) {
			throw new TradistaMessagingBusinessException("Queue name cannot be empty.");
		}
	}

	public static void checkFilterConfig(FilterConfig filterConfig) throws TradistaMessagingBusinessException {
		if (filterConfig == null) {
			throw new TradistaMessagingBusinessException("Filter config cannot be null.");
		}
		if (filterConfig.getName() == null || filterConfig.getName().trim().isEmpty()) {
			throw new TradistaMessagingBusinessException("Filter name cannot be empty.");
		}
		if (filterConfig.getFilterClass() == null || filterConfig.getFilterClass().trim().isEmpty()) {
			throw new TradistaMessagingBusinessException("Filter class cannot be empty.");
		}
	}

	public static void checkRouter(MessageRouter router) throws TradistaMessagingBusinessException {
		if (router == null) {
			throw new TradistaMessagingBusinessException("Router cannot be null.");
		}
		if (router.getName() == null || router.getName().trim().isEmpty()) {
			throw new TradistaMessagingBusinessException("Router name cannot be empty.");
		}
		if (router.getRules() != null) {
			for (RoutingRule rule : router.getRules()) {
				if (rule.getEventType() == null || rule.getEventType().trim().isEmpty()) {
					throw new TradistaMessagingBusinessException("Routing rule event type cannot be empty.");
				}
				if (rule.getDestinationQueue() == null) {
					throw new TradistaMessagingBusinessException("Routing rule destination queue cannot be null.");
				}
			}
		}
	}

	public static void checkEndpoint(MessageEndpoint endpoint) throws TradistaMessagingBusinessException {
		if (endpoint == null) {
			throw new TradistaMessagingBusinessException("Endpoint cannot be null.");
		}
		if (endpoint.getName() == null || endpoint.getName().trim().isEmpty()) {
			throw new TradistaMessagingBusinessException("Endpoint name cannot be empty.");
		}
		if (endpoint.getQueue() == null) {
			throw new TradistaMessagingBusinessException("Endpoint queue cannot be null.");
		}
		if (endpoint.getPollerConfig() != null) {
			if (endpoint.getPollerConfig().getDelayMs() < 0) {
				throw new TradistaMessagingBusinessException("Poller delay cannot be negative.");
			}
			if (endpoint.getPollerConfig().getMaxMessagesPerPoll() <= 0) {
				throw new TradistaMessagingBusinessException("Poller max messages per poll must be greater than 0.");
			}
		}
	}
}
