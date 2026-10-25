package finance.tradista.messaging.persistence.mapper;

import finance.tradista.messaging.model.MessageQueue;
import finance.tradista.messaging.persistence.entity.MessageQueueEntity;

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
 * Mapper converting between domain MessageQueue and persistence
 * MessageQueueEntity.
 * 
 * @author Olivier Asuncion
 */
public final class MessageQueueMapper {

	private MessageQueueMapper() {
	}

	public static MessageQueue toDomain(MessageQueueEntity entity) {
		if (entity == null) {
			return null;
		}
		MessageQueue domain = new MessageQueue(entity.getName());
		domain.setDescription(entity.getDescription());
		domain.setCapacity(entity.getCapacity());
		return domain;
	}

	public static MessageQueueEntity toEntity(MessageQueue domain) {
		if (domain == null) {
			return null;
		}
		MessageQueueEntity entity = new MessageQueueEntity(domain.getName());
		entity.setDescription(domain.getDescription());
		entity.setCapacity(domain.getCapacity());
		return entity;
	}
}
