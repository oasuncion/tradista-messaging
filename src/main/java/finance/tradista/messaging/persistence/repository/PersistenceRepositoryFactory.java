package finance.tradista.messaging.persistence.repository;

import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;

import jakarta.persistence.EntityManagerFactory;

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
 * Factory providing Jakarta Data repositories generated at compile-time by
 * Hibernate.
 * 
 * @author Olivier Asuncion
 */
public final class PersistenceRepositoryFactory {

	private PersistenceRepositoryFactory() {
	}

	public static MessageQueueEntityRepository createMessageQueueRepository(StatelessSession session) {
		return new MessageQueueEntityRepository_(session);
	}

	public static FilterConfigEntityRepository createFilterConfigRepository(StatelessSession session) {
		return new FilterConfigEntityRepository_(session);
	}

	public static MessageRouterEntityRepository createMessageRouterRepository(StatelessSession session) {
		return new MessageRouterEntityRepository_(session);
	}

	public static MessageEndpointEntityRepository createMessageEndpointRepository(StatelessSession session) {
		return new MessageEndpointEntityRepository_(session);
	}

	public static StatelessSession openStatelessSession(EntityManagerFactory emf) {
		SessionFactory sessionFactory = emf.unwrap(SessionFactory.class);
		return sessionFactory.openStatelessSession();
	}
}
