package finance.tradista.messaging.persistence.mapper;

import finance.tradista.messaging.model.FilterConfig;
import finance.tradista.messaging.persistence.entity.FilterConfigEntity;

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
 * Mapper converting between domain FilterConfig and persistence FilterConfigEntity.
 * 
 * @author Olivier Asuncion
 */
public final class FilterConfigMapper {

	private FilterConfigMapper() {
	}

	public static FilterConfig toDomain(FilterConfigEntity entity) {
		if (entity == null) {
			return null;
		}
		FilterConfig domain = new FilterConfig(entity.getName(), entity.getFilterClass());
		domain.setDescription(entity.getDescription());
		return domain;
	}

	public static FilterConfigEntity toEntity(FilterConfig domain) {
		if (domain == null) {
			return null;
		}
		FilterConfigEntity entity = new FilterConfigEntity(domain.getName(), domain.getFilterClass());
		entity.setDescription(domain.getDescription());
		return entity;
	}
}
