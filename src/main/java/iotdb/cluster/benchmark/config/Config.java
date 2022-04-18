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

import iotdb.cluster.benchmark.common.Endpoint;
import iotdb.cluster.benchmark.mode.Mode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Config {
  private GeneralConfig generalConfig = new GeneralConfig();
  private ConfigNodeConfig configNodeConfig = new ConfigNodeConfig();
  private DataNodeConfig dataNodeConfig = new DataNodeConfig();

  public GeneralConfig getGeneralConfig() {
    return generalConfig;
  }

  public void setGeneralConfig(GeneralConfig generalConfig) {
    this.generalConfig = generalConfig;
  }

  public ConfigNodeConfig getConfigNodeConfig() {
    return configNodeConfig;
  }

  public void setConfigNodeConfig(ConfigNodeConfig configNodeConfig) {
    this.configNodeConfig = configNodeConfig;
  }

  public DataNodeConfig getDataNodeConfig() {
    return dataNodeConfig;
  }

  public void setDataNodeConfig(DataNodeConfig dataNodeConfig) {
    this.dataNodeConfig = dataNodeConfig;
  }

  /** get properties from config, one property in one line. */
  public ConfigProperties getShowConfigProperties() {
    ConfigProperties configProperties = new ConfigProperties();

    configProperties.addProperty("GeneralConfig", "mode", generalConfig.mode);
    configProperties.addProperty(
        "GeneralConfig", "storageGroupNumber", generalConfig.storageGroupNumber);
    configProperties.addProperty("GeneralConfig", "deviceNumber", generalConfig.deviceNumber);
    configProperties.addProperty(
        "GeneralConfig", "storageGroupNamePrefix", generalConfig.storageGroupNamePrefix);
    configProperties.addProperty(
        "GeneralConfig", "deviceNamePrefix", generalConfig.deviceNamePrefix);
    configProperties.addProperty("GeneralConfig", "clientNumber", generalConfig.clientNumber);
    configProperties.addProperty("GeneralConfig", "operationNumber", generalConfig.operationNumber);
    configProperties.addProperty(
        "GeneralConfig", "operationProportion", generalConfig.operationProportion);
    configProperties.addProperty("GeneralConfig", "resultPrecision", generalConfig.resultPrecision);
    configProperties.addProperty("GeneralConfig", "dataSeed", generalConfig.dataSeed);
    configProperties.addProperty("GeneralConfig", "logInterval", generalConfig.logInterval);

    configProperties.addProperty("ConfigNodeConfig", "endpoints", configNodeConfig.endpoints);
    configProperties.addProperty("ConfigNodeConfig", "timeOut", configNodeConfig.timeOut);

    configProperties.addProperty("DataNodeConfig", "ipLength", dataNodeConfig.ipLength);
    configProperties.addProperty("DataNodeConfig", "startPort", dataNodeConfig.startPort);
    configProperties.addProperty("DataNodeConfig", "timeOut", dataNodeConfig.timeOut);

    return configProperties;
  }

  /** get all properties from config, one property in one line. */
  public ConfigProperties getAllConfigProperties() {
    ConfigProperties configProperties = getShowConfigProperties();

    return configProperties;
  }

  public static class GeneralConfig {
    private Mode mode = Mode.CONFIG_NODE_REGISTER_AND_QUERY_DATANODE;
    private int storageGroupNumber = 5;
    private int deviceNumber = 5;
    private String storageGroupNamePrefix = "s_";
    private String deviceNamePrefix = "d_";
    private int clientNumber = 5;
    private int operationNumber = 1000;
    private String operationProportion = "1:1";
    private double resultPrecision = 0.1;
    private long dataSeed = 666L;
    private int logInterval = 5;

    public Mode getMode() {
      return mode;
    }

    public void setMode(Mode mode) {
      this.mode = mode;
    }

    public int getStorageGroupNumber() {
      return storageGroupNumber;
    }

    public void setStorageGroupNumber(int storageGroupNumber) {
      this.storageGroupNumber = storageGroupNumber;
    }

    public int getDeviceNumber() {
      return deviceNumber;
    }

    public void setDeviceNumber(int deviceNumber) {
      this.deviceNumber = deviceNumber;
    }

    public String getStorageGroupNamePrefix() {
      return storageGroupNamePrefix;
    }

    public void setStorageGroupNamePrefix(String storageGroupNamePrefix) {
      this.storageGroupNamePrefix = storageGroupNamePrefix;
    }

    public String getDeviceNamePrefix() {
      return deviceNamePrefix;
    }

    public void setDeviceNamePrefix(String deviceNamePrefix) {
      this.deviceNamePrefix = deviceNamePrefix;
    }

    public int getClientNumber() {
      return clientNumber;
    }

    public void setClientNumber(int clientNumber) {
      this.clientNumber = clientNumber;
    }

    public int getOperationNumber() {
      return operationNumber;
    }

    public void setOperationNumber(int operationNumber) {
      this.operationNumber = operationNumber;
    }

    public String getOperationProportion() {
      return operationProportion;
    }

    public void setOperationProportion(String operationProportion) {
      this.operationProportion = operationProportion;
    }

    public double getResultPrecision() {
      return resultPrecision;
    }

    public void setResultPrecision(double resultPrecision) {
      this.resultPrecision = resultPrecision;
    }

    public long getDataSeed() {
      return dataSeed;
    }

    public void setDataSeed(long dataSeed) {
      this.dataSeed = dataSeed;
    }

    public int getLogInterval() {
      return logInterval;
    }

    public void setLogInterval(int logInterval) {
      this.logInterval = logInterval;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GeneralConfig that = (GeneralConfig) o;
      return clientNumber == that.clientNumber
          && operationNumber == that.operationNumber
          && Double.compare(that.resultPrecision, resultPrecision) == 0
          && dataSeed == that.dataSeed
          && Objects.equals(operationProportion, that.operationProportion)
          && logInterval == that.logInterval;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          clientNumber,
          operationNumber,
          operationProportion,
          resultPrecision,
          dataSeed,
          logInterval);
    }
  }

  public static class ConfigNodeConfig {
    private List<Endpoint> endpoints = Collections.singletonList(new Endpoint());
    private int timeOut = 2000;

    public List<Endpoint> getEndpoints() {
      return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
      this.endpoints = endpoints;
    }

    public int getTimeOut() {
      return timeOut;
    }

    public void setTimeOut(int timeOut) {
      this.timeOut = timeOut;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConfigNodeConfig that = (ConfigNodeConfig) o;
      return Objects.equals(endpoints, that.endpoints) && timeOut == that.timeOut;
    }

    @Override
    public int hashCode() {
      return Objects.hash(endpoints, timeOut);
    }
  }

  public static class DataNodeConfig {
    private int ipLength = 10;
    private int startPort = 6667;
    private int timeOut = 2000;

    public int getIpLength() {
      return ipLength;
    }

    public void setIpLength(int ipLength) {
      this.ipLength = ipLength;
    }

    public int getStartPort() {
      return startPort;
    }

    public void setStartPort(int startPort) {
      this.startPort = startPort;
    }

    public int getTimeOut() {
      return timeOut;
    }

    public void setTimeOut(int timeOut) {
      this.timeOut = timeOut;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataNodeConfig that = (DataNodeConfig) o;
      return ipLength == that.ipLength && startPort == that.startPort && timeOut == that.timeOut;
    }

    @Override
    public int hashCode() {
      return Objects.hash(ipLength, startPort, timeOut);
    }
  }
}
