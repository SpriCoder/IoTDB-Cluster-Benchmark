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

package iotdb.cluster.benchmark.operation;

import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OperationController {
  private static final Logger logger = LoggerFactory.getLogger(OperationController.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private List<Operation> operations;
  private Random random;
  private List<Double> proportion = new ArrayList<>();

  public OperationController() {
    operations = Operation.getAllOperations();
    this.random = new Random(config.getGeneralConfig().getDataSeed());
    String[] split = config.getGeneralConfig().getOperationProportion().split(":");
    if (split.length != operations.size()) {
      logger.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[operations.size()];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        logger.error("The sum of operation proportions is zero!");
      }
    }
  }

  /** Get next Operation type */
  public Operation getNextOperationType() {
    // p contains cumulative probability
    double[] p = new double[operations.size() + 1];
    p[0] = 0.0;
    for (int i = 1; i <= operations.size(); i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    // use random to getNextOperationType
    double rand = random.nextDouble();
    for (int i = 0; i < operations.size(); i++) {
      if (rand >= p[i] && rand <= p[i + 1]) {
        return operations.get(i);
      }
    }
    return Operation.REGISTER_DATANODE;
  }
}
