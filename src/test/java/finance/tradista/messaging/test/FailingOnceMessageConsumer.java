package finance.tradista.messaging.test;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Consumer that fails on the first delivery attempt to test message rollback
 * and redelivery.
 * 
 * @author Olivier Asuncion
 */
public class FailingOnceMessageConsumer implements MessageConsumer<Serializable> {

	private static final AtomicInteger ATTEMPTS = new AtomicInteger(0);
	private static volatile CountDownLatch latch;

	public static void reset(int expectedAttempts) {
		ATTEMPTS.set(0);
		latch = new CountDownLatch(expectedAttempts);
	}

	public static boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch != null && latch.await(timeout, unit);
	}

	public static int getAttempts() {
		return ATTEMPTS.get();
	}

	@Override
	public void onMessage(Message<Serializable> message) {
		int current = ATTEMPTS.incrementAndGet();
		if (latch != null) {
			latch.countDown();
		}
		if (current == 1) {
			throw new RuntimeException("Simulated business processing failure on first attempt");
		}
	}
}
