package finance.tradista.messaging.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import finance.tradista.messaging.model.Message;
import finance.tradista.messaging.model.MessageConsumer;

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
 * Test consumer collecting consumed messages in a static list for test
 * assertions.
 * 
 * @author Olivier Asuncion
 */
public class CollectingMessageConsumer implements MessageConsumer<Serializable> {

	private static final List<Message<Serializable>> RECEIVED = Collections.synchronizedList(new ArrayList<>());
	private static volatile CountDownLatch latch;

	public static void resetLatch(int count) {
		latch = new CountDownLatch(count);
	}

	public static boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch != null && latch.await(timeout, unit);
	}

	@Override
	public void onMessage(Message<Serializable> message) {
		RECEIVED.add(message);
		if (latch != null) {
			latch.countDown();
		}
	}

	public static List<Message<Serializable>> getReceived() {
		return RECEIVED;
	}

	public static void clear() {
		RECEIVED.clear();
	}
}
