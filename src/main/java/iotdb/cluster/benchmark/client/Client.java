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
import iotdb.cluster.benchmark.operation.OperationController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class Client implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  protected static Config config = ConfigDescriptor.getInstance().getConfig();

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
  /** Measurement */
  protected Measurement measurement;
  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;

  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.clientThreadId = id;
    this.operationController = new OperationController();
    this.measurement = new Measurement();
  }

  @Override
  public void run() {
    try {
      try {
        // wait for that all dataClients start test simultaneously
        barrier.await();

        String currentThread = Thread.currentThread().getName();
        // print current progress periodically
        service.scheduleAtFixedRate(
            () -> {
              String percent =
                  String.format("%.2f", (operationNumber) * 100.0D / totalOperationNumber);
              logger.info("{} {}% workload is done.", currentThread, percent);
            },
            1,
            config.getGeneralConfig().getLogInterval(),
            TimeUnit.SECONDS);

        for (operationNumber = 0; operationNumber < totalOperationNumber; operationNumber++) {
          Operation operation = operationController.getNextOperationType();
          Status status = new Status(false);
          long start = System.currentTimeMillis();
          switch (operation) {
            case WRITE:
              status = write();
              break;
            case QUERY:
              status = query();
              break;
            default:
              logger.error("Unknown Operation Type: " + operation);
              status = new Status(false, 1);
          }
          status.setTimeCost(System.currentTimeMillis() - start);
          if (status.isOk()) {
            measurement.addOperationLatency(operation, status.getTimeCost());
            measurement.addOkOperationNum(operation);
            measurement.addOkPointNum(operation, 1);
          } else {
            measurement.addFailOperationNum(operation);
            measurement.addFailPointNum(operation, 1);
          }
        }
        service.shutdown();
      } catch (Exception e) {
        logger.error("Unexpected error: ", e);
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  private Status write() {
    return new Status(true, 1);
  }

  private Status query() {
    return new Status(true, 1);
  }

  public Measurement getMeasurement() {
    return measurement;
  }
}
