package finance.tradista.messaging.persistence.mapper;

import java.util.HashSet;
import java.util.Set;

import finance.tradista.messaging.model.MessageRouter;
import finance.tradista.messaging.model.RoutingRule;
import finance.tradista.messaging.persistence.entity.MessageQueueEntity;
import finance.tradista.messaging.persistence.entity.MessageRouterEntity;
import finance.tradista.messaging.persistence.entity.RoutingRuleEntity;

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
 * Mapper converting between domain MessageRouter and persistence
 * MessageRouterEntity.
 * 
 * @author Olivier Asuncion
 */
public final class MessageRouterMapper {

	private MessageRouterMapper() {
	}

	public static MessageRouter toDomain(MessageRouterEntity entity) {
		if (entity == null) {
			return null;
		}
		MessageRouter domain = new MessageRouter(entity.getName());
		domain.setDescription(entity.getDescription());
		if (entity.getDefaultOutputQueue() != null) {
			domain.setDefaultOutputQueue(MessageQueueMapper.toDomain(entity.getDefaultOutputQueue()));
		}
		if (entity.getRules() != null) {
			Set<RoutingRule> domainRules = new HashSet<>();
			for (RoutingRuleEntity ruleEntity : entity.getRules()) {
				RoutingRule domainRule = new RoutingRule();
				domainRule.setRouter(domain);
				domainRule.setEventType(ruleEntity.getEventType());
				domainRule.setDestinationQueue(MessageQueueMapper.toDomain(ruleEntity.getDestinationQueue()));
				domainRules.add(domainRule);
			}
			domain.setRules(domainRules);
		}
		return domain;
	}

	public static MessageRouterEntity toEntity(MessageRouter domain, MessageQueueEntity defaultOutputQueueEntity) {
		if (domain == null) {
			return null;
		}
		MessageRouterEntity entity = new MessageRouterEntity(domain.getName());
		entity.setDescription(domain.getDescription());
		entity.setDefaultOutputQueue(defaultOutputQueueEntity);
		return entity;
	}
}
