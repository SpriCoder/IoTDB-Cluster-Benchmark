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
import iotdb.cluster.benchmark.measurement.Status;
import iotdb.cluster.benchmark.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

public class Client implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  protected static Config config = ConfigDescriptor.getInstance().getConfig();

  /** The id of client */
  protected final int clientThreadId;

  /** Measurement */
  protected Measurement measurement;

  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;

  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.clientThreadId = id;
    this.measurement = new Measurement();
  }

  @Override
  public void run() {
    try {
      try {
        // wait for that all dataClients start test simultaneously
        barrier.await();

        String currentThread = Thread.currentThread().getName();

        Random test = new Random(config.getGeneralConfig().getDataSeed());

        // TODO test
        for (int i = 0; i < 100; i++) {
          if (i % 10 == 0) {
            logger.info(currentThread + ":" + i);
          }
          Thread.sleep(10);
          Status status = new Status(true);
          int number = test.nextInt(100);
          status.setTimeCost(number);
          measurement.addOperationLatency(Operation.CONFIG_NODE, status.getTimeCost());
          measurement.addOkOperationNum(Operation.CONFIG_NODE);
          measurement.addOkPointNum(Operation.CONFIG_NODE, number);
        }
      } catch (Exception e) {
        logger.error("Unexpected error: ", e);
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  public Measurement getMeasurement() {
    return measurement;
  }
}
