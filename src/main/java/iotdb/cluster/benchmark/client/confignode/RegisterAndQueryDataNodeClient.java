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

package iotdb.cluster.benchmark.client.confignode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.rpc.TSStatusCode;

import iotdb.cluster.benchmark.measurement.Status;
import iotdb.cluster.benchmark.operation.Operation;
import iotdb.cluster.benchmark.tool.DataNodeEndpointTool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class RegisterAndQueryDataNodeClient extends ConfigNodeClient {
  private static final Logger logger =
      LoggerFactory.getLogger(RegisterAndQueryDataNodeClient.class);

  public RegisterAndQueryDataNodeClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected boolean doOperation(Operation operation) {
    Status status = new Status(false);
    long start = System.currentTimeMillis();
    switch (operation) {
      case REGISTER_DATANODE:
        status = write();
        break;
      case QUERY_DATANODE:
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
    return true;
  }

  private Status write() {
    TEndPoint endPoint = DataNodeEndpointTool.getNextDataNodeEndPoint();
    TDataNodeInfo dataNodeInfo =
        new TDataNodeInfo(
            new TDataNodeLocation(1, endPoint, endPoint, endPoint, endPoint), 1, 1024 * 512);
    TDataNodeRegisterReq req = new TDataNodeRegisterReq(dataNodeInfo);
    Status status;
    try {
      status = doWrite(endPoint, req);
    } catch (TException e) {
      if (reconnect()) {
        try {
          status = doWrite(endPoint, req);
        } catch (TException e1) {
          logger.error(e.getMessage());
          status = new Status(false, e, e.getMessage());
        }
      } else {
        logger.error(MSG_RECONNECTION_FAIL);
        status = new Status(false, e, e.getMessage());
      }
    }
    return status;
  }

  private Status doWrite(TEndPoint endPoint, TDataNodeRegisterReq req) throws TException {
    TDataNodeRegisterResp resp = configNodeClient.registerDataNode(req);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
      dataNodeId.add(resp.dataNodeId);
      idAndEndPointMap.put(resp.dataNodeId, endPoint);
      logger.debug(
          "Client-"
              + clientThreadId
              + " register datanode-"
              + resp.dataNodeId
              + " endpoint{ip="
              + endPoint.getIp()
              + ", port="
              + endPoint.getPort()
              + "}");
      return new Status(true, 1);
    } else {
      return new Status(false);
    }
  }

  private Status query() {
    Integer queryDataNodeId = getNextQueryDataNodeId();
    Status status;
    try {
      TDataNodeInfoResp msgMap = configNodeClient.getDataNodeInfo(queryDataNodeId);
      status = new Status(true, msgMap.dataNodeInfoMap.size());
    } catch (TException e) {
      if (reconnect()) {
        try {
          TDataNodeInfoResp msgMap = configNodeClient.getDataNodeInfo(queryDataNodeId);
          status = new Status(true, msgMap.dataNodeInfoMap.size());
        } catch (TException e1) {
          logger.error(e.getMessage());
          status = new Status(false, e, e.getMessage());
        }
      } else {
        logger.error(MSG_RECONNECTION_FAIL);
        status = new Status(false, e, e.getMessage());
      }
    }
    return status;
  }

  private Integer getNextQueryDataNodeId() {
    if (dataNodeId.size() == 0) {
      return -1;
    }
    return dataNodeId.get(random.nextInt(dataNodeId.size()));
  }
}
