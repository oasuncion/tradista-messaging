package finance.tradista.messaging.persistence.repository;

import java.util.List;
import java.util.Optional;

import finance.tradista.messaging.persistence.entity.MessageRouterEntity;
import jakarta.data.repository.DataRepository;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;

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
 * Jakarta Data Repository for MessageRouterEntity using Jakarta Query expressions.
 * 
 * @author Olivier Asuncion
 */
@Repository
public interface MessageRouterEntityRepository extends DataRepository<MessageRouterEntity, Long> {

	@Save
	<S extends MessageRouterEntity> S save(S entity);

	@Insert
	<S extends MessageRouterEntity> S insert(S entity);

	@Update
	<S extends MessageRouterEntity> S update(S entity);

	@Find
	Optional<MessageRouterEntity> findById(Long id);

	/**
	 * Jakarta Query expression to find a MessageRouterEntity by its name with rules fetched.
	 */
	@Query("SELECT r FROM MessageRouterEntity r LEFT JOIN FETCH r.rules WHERE r.name = :name")
	Optional<MessageRouterEntity> findByName(@Param("name") String name);

	/**
	 * Jakarta Query expression returning all routers ordered by name with rules fetched.
	 */
	@Query("SELECT DISTINCT r FROM MessageRouterEntity r LEFT JOIN FETCH r.rules ORDER BY r.name ASC")
	List<MessageRouterEntity> findAllOrdered();

	@Delete
	void delete(MessageRouterEntity entity);

	@Delete
	void deleteById(Long id);
}
