/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package iotdb.cluster.benchmark.client;

import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import iotdb.cluster.benchmark.measurement.Measurement;
import iotdb.cluster.benchmark.operation.OperationController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class Client implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  protected static Config config = ConfigDescriptor.getInstance().getConfig();

  protected static final String MSG_RECONNECTION_FAIL = "Failed to reconnect config node";

  /** The id of client */
  protected final int clientThreadId;
  /** The current number of Operation */
  protected int operationNumber = 0;
  /** The total number of Operation */
  protected final int totalOperationNumber = config.getGeneralConfig().getOperationNumber();
  /** Log related */
  protected final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  /** The controller of operations */
  protected final OperationController operationController;
  /** The random of query */
  protected final Random random = new Random(config.getGeneralConfig().getDataSeed());
  /** Measurement */
  protected Measurement measurement;
  /** Control the end of client */
  protected final CountDownLatch countDownLatch;

  protected final CyclicBarrier barrier;

  protected Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.clientThreadId = id;
    this.operationController = new OperationController();
    this.measurement = new Measurement();
  }

  public static Client getInstance(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    return new RegisterAndQueryDataNodeClient(id, countDownLatch, barrier);
  }

  @Override
  public void run() {
    try {
      try {
        if (!init()) {
          return;
        }
        barrier.await();

        // print current progress periodically
        String currentThread = Thread.currentThread().getName();
        service.scheduleAtFixedRate(
            () -> {
              String percent =
                  String.format("%.2f", (operationNumber) * 100.0D / totalOperationNumber);
              logger.info("{} {}% workload is done.", currentThread, percent);
            },
            1,
            config.getGeneralConfig().getLogInterval(),
            TimeUnit.SECONDS);

        doTest();
        close();
        service.shutdown();
      } catch (Exception e) {
        logger.error("Unexpected error: ", e);
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  /** Init the client, such as connect */
  protected abstract boolean init();

  /** Do Test */
  protected abstract boolean doTest();

  /** Close the client */
  protected abstract boolean close();

  public Measurement getMeasurement() {
    return measurement;
  }
}
