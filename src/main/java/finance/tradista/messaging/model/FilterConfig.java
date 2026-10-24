package finance.tradista.messaging.model;

import java.io.Serializable;
import java.util.Objects;

import finance.tradista.messaging.exception.TradistaMessagingTechnicalException;

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
 * Filter definition specifying the filter name and implementing class.
 * Pure domain model without persistence dependencies.
 * 
 * @author Olivier Asuncion
 */
public class FilterConfig implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private String name;
	private String filterClass;
	private String description;

	private transient MessageFilter<?> instance;

	public FilterConfig() {
	}

	public FilterConfig(String name, String filterClass) {
		this.name = name;
		this.filterClass = filterClass;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFilterClass() {
		return filterClass;
	}

	public void setFilterClass(String filterClass) {
		this.filterClass = filterClass;
		this.instance = null;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@SuppressWarnings("unchecked")
	public <T> MessageFilter<T> getInstance() {
		if (instance == null && filterClass != null) {
			try {
				Class<?> clazz = Class.forName(filterClass);
				instance = (MessageFilter<?>) clazz.getDeclaredConstructor().newInstance();
			} catch (Exception e) {
				throw new TradistaMessagingTechnicalException("Cannot instantiate filter class " + filterClass, e);
			}
		}
		return (MessageFilter<T>) instance;
	}

	@Override
	public FilterConfig clone() {
		try {
			FilterConfig clone = (FilterConfig) super.clone();
			clone.instance = null;
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof FilterConfig other) {
			return Objects.equals(name, other.name);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return name;
	}
}
