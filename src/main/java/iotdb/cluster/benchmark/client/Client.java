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

import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import iotdb.cluster.benchmark.common.Endpoint;
import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import iotdb.cluster.benchmark.measurement.Measurement;
import iotdb.cluster.benchmark.measurement.Status;
import iotdb.cluster.benchmark.operation.Operation;
import iotdb.cluster.benchmark.operation.OperationController;
import iotdb.cluster.benchmark.tool.DataNodeEndpointTool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
  /** The endpoint of config node */
  protected Endpoint configEndpoints;
  /** The client of config node */
  protected ConfigIService.Client configNodeClient;
  /** The list of data node id */
  protected List<Integer> dataNodeId = new ArrayList<>();
  /** The map of data node */
  protected Map<Integer, EndPoint> idAndEndPointMap = new HashMap<>();
  /** Log related */
  protected final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  /** The controller of operations */
  protected final OperationController operationController;
  /** The random of query */
  protected final Random random = new Random(config.getGeneralConfig().getDataSeed());
  /** Measurement */
  protected Measurement measurement;
  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;

  public Client(
      int id, Endpoint configEndpoint, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.configEndpoints = configEndpoint;
    this.barrier = barrier;
    this.clientThreadId = id;
    this.operationController = new OperationController();
    this.measurement = new Measurement();
  }

  @Override
  public void run() {
    try {
      try {
        TTransport transport =
            RpcTransportFactory.INSTANCE.getTransport(
                configEndpoints.getHost(),
                configEndpoints.getPort(),
                config.getConfigNodeConfig().getTimeOut());
        transport.open();
        configNodeClient = new ConfigIService.Client(new TBinaryProtocol(transport));
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
        // TODO release client
        configNodeClient = null;
        transport.close();
        service.shutdown();
      } catch (Exception e) {
        logger.error("Unexpected error: ", e);
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  private Status write() {
    EndPoint endPoint = DataNodeEndpointTool.getNextDataNodeEndPoint();
    DataNodeRegisterReq req = new DataNodeRegisterReq(endPoint);
    try {
      DataNodeRegisterResp resp = configNodeClient.registerDataNode(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.registerResult.getCode()) {
        dataNodeId.add(resp.dataNodeID);
        idAndEndPointMap.put(resp.dataNodeID, endPoint);
        logger.info(
            "Register datanode-"
                + resp.dataNodeID
                + "endpoint{ip="
                + endPoint.getIp()
                + ", port="
                + endPoint.getPort()
                + "}");
        return new Status(true, 1);
      } else {
        return new Status(false);
      }
    } catch (TException e) {
      logger.error(e.getMessage());
      return new Status(false, e, e.getMessage());
    }
  }

  private Status query() {
    Integer queryDataNodeId = getNextQueryDataNodeId();
    try {
      Map<Integer, DataNodeMessage> msgMap = configNodeClient.getDataNodesMessage(queryDataNodeId);
      return new Status(true, msgMap.size());
    } catch (TException e) {
      logger.error(e.getMessage());
      return new Status(false, e, e.getMessage());
    }
  }

  private Integer getNextQueryDataNodeId() {
    if (dataNodeId.size() == 0) {
      return -1;
    }
    return dataNodeId.get(random.nextInt(dataNodeId.size()));
  }

  public Measurement getMeasurement() {
    return measurement;
  }
}
