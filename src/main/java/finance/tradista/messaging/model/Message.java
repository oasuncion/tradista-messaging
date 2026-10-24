package finance.tradista.messaging.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

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
 * Generic message envelope containing payload, headers and metadata.
 * 
 * @param <T> the type of payload
 * @author Olivier Asuncion
 */
public class Message<T extends Serializable> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String id;
	private final Instant timestamp;
	private final T payload;
	private final Map<String, Serializable> headers;

	public Message(T payload) {
		this(payload, Collections.emptyMap());
	}

	public Message(T payload, Map<String, Serializable> headers) {
		this(UUID.randomUUID().toString(), Instant.now(), payload, headers);
	}

	public Message(String id, Instant timestamp, T payload, Map<String, Serializable> headers) {
		this.id = id != null ? id : UUID.randomUUID().toString();
		this.timestamp = timestamp != null ? timestamp : Instant.now();
		this.payload = payload;
		this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
	}

	public String getId() {
		return id;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public T getPayload() {
		return payload;
	}

	public Map<String, Serializable> getHeaders() {
		return Collections.unmodifiableMap(headers);
	}

	public Serializable getHeader(String headerName) {
		return headers.get(headerName);
	}

	public void setHeader(String headerName, Serializable value) {
		this.headers.put(headerName, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		Message<?> other = (Message<?>) obj;
		return Objects.equals(id, other.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public String toString() {
		return "Message{" + "id='" + id + '\'' + ", timestamp=" + timestamp + ", payload=" + payload + '}';
	}
}
