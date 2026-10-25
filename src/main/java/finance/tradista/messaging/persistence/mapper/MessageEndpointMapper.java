package finance.tradista.messaging.persistence.mapper;

import finance.tradista.messaging.model.MessageEndpoint;
import finance.tradista.messaging.model.PollerConfig;
import finance.tradista.messaging.persistence.entity.FilterConfigEntity;
import finance.tradista.messaging.persistence.entity.MessageEndpointEntity;
import finance.tradista.messaging.persistence.entity.MessageQueueEntity;
import finance.tradista.messaging.persistence.entity.PollerConfigEmbeddable;

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
 * Mapper converting between domain MessageEndpoint and persistence
 * MessageEndpointEntity.
 * 
 * @author Olivier Asuncion
 */
public final class MessageEndpointMapper {

	private MessageEndpointMapper() {
	}

	public static MessageEndpoint toDomain(MessageEndpointEntity entity) {
		if (entity == null) {
			return null;
		}
		MessageEndpoint domain = new MessageEndpoint();
		domain.setName(entity.getName());
		domain.setDescription(entity.getDescription());
		if (entity.getQueue() != null) {
			domain.setQueue(MessageQueueMapper.toDomain(entity.getQueue()));
		}
		if (entity.getFilterConfig() != null) {
			domain.setFilterConfig(FilterConfigMapper.toDomain(entity.getFilterConfig()));
		}
		domain.setConsumerClass(entity.getConsumerClass());
		if (entity.getPollerConfig() != null) {
			domain.setPollerConfig(new PollerConfig(entity.getPollerConfig().getDelayMs(),
					entity.getPollerConfig().getMaxMessagesPerPoll()));
		}
		domain.setStatus(entity.getStatus());
		return domain;
	}

	public static MessageEndpointEntity toEntity(MessageEndpoint domain, MessageQueueEntity queueEntity,
			FilterConfigEntity filterConfigEntity) {
		if (domain == null) {
			return null;
		}
		MessageEndpointEntity entity = new MessageEndpointEntity();
		entity.setName(domain.getName());
		entity.setDescription(domain.getDescription());
		entity.setQueue(queueEntity);
		entity.setFilterConfig(filterConfigEntity);
		entity.setConsumerClass(domain.getConsumerClass());
		if (domain.getPollerConfig() != null) {
			entity.setPollerConfig(new PollerConfigEmbeddable(domain.getPollerConfig().getDelayMs(),
					domain.getPollerConfig().getMaxMessagesPerPoll()));
		}
		entity.setStatus(domain.getStatus());
		return entity;
	}
}
