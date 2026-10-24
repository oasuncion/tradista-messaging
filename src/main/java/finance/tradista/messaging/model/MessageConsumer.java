package finance.tradista.messaging.model;

import java.io.Serializable;

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
 * Functional interface for message listeners / consumers.
 *
 * @param <T> event or message payload type
 * @author Olivier Asuncion
 */
@FunctionalInterface
public interface MessageConsumer<T extends Serializable> {

	/**
	 * Consumes the received message.
	 *
	 * @param message the message received
	 */
	void onMessage(Message<T> message);
}
