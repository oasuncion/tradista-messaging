package finance.tradista.messaging.persistence.repository;

import java.util.List;
import java.util.Optional;

import finance.tradista.messaging.persistence.entity.FilterConfigEntity;
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
 * Jakarta Data Repository for FilterConfigEntity using Jakarta Query expressions.
 * 
 * @author Olivier Asuncion
 */
@Repository
public interface FilterConfigEntityRepository extends DataRepository<FilterConfigEntity, Long> {

	@Save
	<S extends FilterConfigEntity> S save(S entity);

	@Insert
	<S extends FilterConfigEntity> S insert(S entity);

	@Update
	<S extends FilterConfigEntity> S update(S entity);

	@Find
	Optional<FilterConfigEntity> findById(Long id);

	/**
	 * Jakarta Query expression to find a FilterConfigEntity by its unique name.
	 */
	@Query("WHERE name = :name")
	Optional<FilterConfigEntity> findByName(@Param("name") String name);

	/**
	 * Jakarta Query expression returning all filter configs ordered by name.
	 */
	@Query("ORDER BY name ASC")
	List<FilterConfigEntity> findAllOrdered();

	@Delete
	void delete(FilterConfigEntity entity);

	@Delete
	void deleteById(Long id);
}
