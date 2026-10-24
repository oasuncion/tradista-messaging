package finance.tradista.messaging.model;

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
 * Functional interface for filtering events/messages.
 *
 * @param <T> event or payload type
 * @author Olivier Asuncion
 */
@FunctionalInterface
public interface MessageFilter<T> {

	/**
	 * Evaluates whether the given message or event matches this filter.
	 *
	 * @param message the message/event to evaluate
	 * @return true if accepted, false otherwise
	 */
	boolean accept(T message);
}
