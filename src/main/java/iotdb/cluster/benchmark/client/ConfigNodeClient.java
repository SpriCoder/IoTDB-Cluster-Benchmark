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
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import iotdb.cluster.benchmark.common.Endpoint;
import iotdb.cluster.benchmark.config.Constants;
import iotdb.cluster.benchmark.operation.Operation;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class ConfigNodeClient extends Client {
  private static final Logger logger =
      LoggerFactory.getLogger(RegisterAndQueryDataNodeClient.class);
  /** The endpoint of config node */
  protected Endpoint configEndpoints;
  /** The transport of config node client */
  protected TTransport transport;
  /** The client of config node */
  protected ConfigIService.Client configNodeClient;
  /** The list of data node id */
  protected List<Integer> dataNodeId = new ArrayList<>();
  /** The map of data node */
  protected Map<Integer, EndPoint> idAndEndPointMap = new HashMap<>();

  protected ConfigNodeClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
    this.configEndpoints = config.getConfigNodeConfig().getEndpoints().get(0);
  }

  @Override
  protected boolean init() {
    if (!openTransport()) {
      return false;
    }
    configNodeClient = new ConfigIService.Client(new TBinaryProtocol(transport));
    // wait for that all dataClients start test simultaneously
    return true;
  }

  private boolean openTransport() {
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              configEndpoints.getHost(),
              configEndpoints.getPort(),
              config.getConfigNodeConfig().getTimeOut());
      transport.open();
    } catch (TTransportException tTransportException) {
      logger.error(MSG_RECONNECTION_FAIL);
      return false;
    }
    return true;
  }

  @Override
  protected boolean doTest() {
    for (operationNumber = 0; operationNumber < totalOperationNumber; operationNumber++) {
      Operation operation = operationController.getNextOperationType();
      doOperation(operation);
    }
    return true;
  }

  /** Do one operation */
  protected abstract boolean doOperation(Operation operation);

  protected boolean reconnect() {
    boolean flag = false;
    for (int i = 0; i < Constants.RETRY_TIME; i++) {
      try {
        if (transport != null) {
          transport.close();
          openTransport();
          configNodeClient = new ConfigIService.Client(new TBinaryProtocol(transport));
          flag = true;
          break;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(Constants.RETRY_INTERVAL_MS);
        } catch (InterruptedException e1) {
          logger.error("reconnect is interrupted", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }

  @Override
  protected boolean close() {
    configNodeClient = null;
    transport.close();
    return true;
  }
}
