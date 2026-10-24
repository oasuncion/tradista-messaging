package finance.tradista.messaging.persistence.repository;

import java.util.List;
import java.util.Optional;

import finance.tradista.messaging.persistence.entity.MessageEndpointEntity;
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
 * Jakarta Data Repository for MessageEndpointEntity using Jakarta Query expressions.
 * 
 * @author Olivier Asuncion
 */
@Repository
public interface MessageEndpointEntityRepository extends DataRepository<MessageEndpointEntity, Long> {

	@Save
	<S extends MessageEndpointEntity> S save(S entity);

	@Insert
	<S extends MessageEndpointEntity> S insert(S entity);

	@Update
	<S extends MessageEndpointEntity> S update(S entity);

	@Find
	Optional<MessageEndpointEntity> findById(Long id);

	/**
	 * Jakarta Query expression to find a MessageEndpointEntity by its unique name.
	 */
	@Query("WHERE name = :name")
	Optional<MessageEndpointEntity> findByName(@Param("name") String name);

	/**
	 * Jakarta Query expression returning all endpoints ordered by name.
	 */
	@Query("ORDER BY name ASC")
	List<MessageEndpointEntity> findAllOrdered();

	@Delete
	void delete(MessageEndpointEntity entity);

	@Delete
	void deleteById(Long id);
}
