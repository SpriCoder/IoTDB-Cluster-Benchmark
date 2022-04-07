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

package iotdb.cluster.benchmark.mode;

import iotdb.cluster.benchmark.client.Client;
import iotdb.cluster.benchmark.common.Endpoint;
import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import iotdb.cluster.benchmark.measurement.Measurement;
import iotdb.cluster.benchmark.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BaseMode {
  private static final Logger logger = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;

  protected ExecutorService executorService =
      Executors.newFixedThreadPool(config.getGeneralConfig().getClientNumber());
  protected CountDownLatch downLatch =
      new CountDownLatch(config.getGeneralConfig().getClientNumber());
  protected CyclicBarrier barrier = new CyclicBarrier(config.getGeneralConfig().getClientNumber());
  protected List<Client> clients = new ArrayList<>();

  protected Measurement measurement = new Measurement();
  protected long start = 0;

  protected boolean preCheck() {
    return true;
  }

  /** Start benchmark */
  public void run() {
    if (!preCheck()) {
      return;
    }
    for (int i = 0; i < config.getGeneralConfig().getClientNumber(); i++) {
      Endpoint configEndpoint = config.getConfigNodeConfig().getEndpoints().get(0);
      Client client = new Client(i, configEndpoint, downLatch, barrier);
      clients.add(client);
    }
    for (Client client : clients) {
      executorService.submit(client);
    }
    start = System.nanoTime();
    executorService.shutdown();
    try {
      // wait for all dataClients finish test
      downLatch.await();
    } catch (InterruptedException e) {
      logger.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    postCheck();
  }

  protected void postCheck() {
    List<Operation> operations = Operation.getAllOperations();
    List<Measurement> threadsMeasurements = new ArrayList<>();
    finalMeasure(measurement, threadsMeasurements, start, clients, operations);
  }

  /** Save measure */
  protected static void finalMeasure(
      Measurement measurement,
      List<Measurement> threadsMeasurements,
      long st,
      List<Client> clients,
      List<Operation> operations) {
    long en = System.nanoTime();
    logger.info("All dataClients finished.");
    // sum up all the measurements and calculate statistics
    measurement.setElapseTime((en - st) / NANO_TO_SECOND);
    for (Client client : clients) {
      threadsMeasurements.add(client.getMeasurement());
    }
    for (Measurement m : threadsMeasurements) {
      measurement.mergeMeasurement(m);
    }
    // output results
    measurement.showConfigs();
    // must call calculateMetrics() before using the Metrics
    measurement.calculateMetrics(operations);
    if (operations.size() != 0) {
      measurement.showMeasurements(operations);
      measurement.showMetrics(operations);
    }
    measurement.outputCSV();
  }
}
