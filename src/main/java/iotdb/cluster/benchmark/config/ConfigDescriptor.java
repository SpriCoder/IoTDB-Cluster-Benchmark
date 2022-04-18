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

package iotdb.cluster.benchmark.config;

import iotdb.cluster.benchmark.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.exit;

public class ConfigDescriptor {
  public static final Logger logger = LoggerFactory.getLogger(ConfigDescriptor.class);
  private Config config;

  private ConfigDescriptor() {
    loadProps();
  }

  /** find the config file path. */
  private String getPropsUrl() {
    String url = System.getProperty(Constants.BENCHMARK_CONF, null);
    if (url == null) {
      logger.warn(
          "Cannot find BENCHMARK_CONF environment variable when loading "
              + "config file {}, use default configuration",
          Constants.CONFIG_NAME);
      return null;
    } else {
      url += (File.separatorChar + Constants.CONFIG_NAME);
    }
    return url;
  }

  /** Load a property file and set MetricConfig variables. If not found file, use default value. */
  private void loadProps() {
    String url = getPropsUrl();
    Constructor constructor = new Constructor(Config.class);
    Yaml yaml = new Yaml(constructor);
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(url)) {
        logger.info("Start to read config file {}", url);
        config = (Config) yaml.load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file : {}, use default config.", url, e);
      }
    } else {
      logger.warn("Fail to find config file, use default");
      config = new Config();
    }
    if (!preCheck()) {
      exit(1);
    }
  }

  public Config getConfig() {
    return config;
  }

  private static class ConfigDescriptorHolder {
    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }

  public static ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  private boolean preCheck() {
    String[] operations = config.getGeneralConfig().getOperationProportion().split(":");
    if (operations.length != Operation.values().length) {
      logger.error("Please check OperationProportion format.");
      return false;
    }
    double operationCheck = 0.0;
    for (int i = 0; i < operations.length; i++) {
      operationCheck += Double.parseDouble(operations[i]);
    }
    switch (config.getGeneralConfig().getMode()) {
      case CONFIG_NODE_REGISTER_AND_QUERY_DATANODE:
        operationCheck -= Double.parseDouble(operations[Operation.REGISTER_DATANODE.ordinal()]);
        operationCheck -= Double.parseDouble(operations[Operation.QUERY_DATANODE.ordinal()]);
        break;
      case CONFIG_NODE_OPERATE_PARTITION:
        operationCheck -=
            Double.parseDouble(operations[Operation.GET_OR_CREATE_SCHEMA_PARTITION.ordinal()]);
        operationCheck -= Double.parseDouble(operations[Operation.GET_SCHEMA_PARTITION.ordinal()]);
        operationCheck -=
            Double.parseDouble(operations[Operation.GET_OR_CREATE_DATA_PARTITION.ordinal()]);
        operationCheck -= Double.parseDouble(operations[Operation.GET_DATA_PARTITION.ordinal()]);
        break;
      default:
        logger.error("Please check mode.");
        return false;
    }
    if (operationCheck > 1e-7) {
      logger.error("Please check mode and operation proportion.");
      return false;
    }
    return true;
  }
}
