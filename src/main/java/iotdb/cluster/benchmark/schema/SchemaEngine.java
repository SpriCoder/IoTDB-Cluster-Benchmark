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

package iotdb.cluster.benchmark.schema;

import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import iotdb.cluster.benchmark.tool.MetaTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaEngine.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String UNKNOWN_DEVICE = "Unknown device: %s";
  /** DeviceSchema for each client */
  private static final Map<Integer, List<DeviceSchema>> CLIENT_DATA_SCHEMA =
      new ConcurrentHashMap<>();
  /** Name mapping of DeviceSchema */
  private static final Map<String, DeviceSchema> NAME_DATA_SCHEMA = new ConcurrentHashMap<>();
  /** The singleton of schemaEngine */
  private static SchemaEngine schemaEngine = null;

  private SchemaEngine() {
    int eachClientDeviceNum =
        config.getGeneralConfig().getDeviceNumber() / config.getGeneralConfig().getClientNumber();
    // The part that cannot be divided equally is given to dataClients with a smaller number
    int leftClientDeviceNum =
        config.getGeneralConfig().getDeviceNumber() % config.getGeneralConfig().getClientNumber();
    int deviceId = MetaTool.getFirstDeviceIndex();
    for (int clientId = 0; clientId < config.getGeneralConfig().getClientNumber(); clientId++) {
      int deviceNumber =
          (clientId < leftClientDeviceNum) ? eachClientDeviceNum + 1 : eachClientDeviceNum;
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema = new DeviceSchema(deviceId);
        NAME_DATA_SCHEMA.put(deviceSchema.getDevice(), deviceSchema);
        deviceSchemaList.add(deviceSchema);
        deviceId++;
      }
      CLIENT_DATA_SCHEMA.put(clientId, deviceSchemaList);
    }
  }

  /** Get DeviceSchema by device */
  public DeviceSchema getDeviceSchemaByName(String deviceName) {
    try {
      return NAME_DATA_SCHEMA.get(deviceName);
    } catch (Exception e) {
      LOGGER.warn(String.format(UNKNOWN_DEVICE, deviceName));
      return null;
    }
  }

  /** Get DeviceSchema by clientId */
  public List<DeviceSchema> getDeviceSchemaByClientId(int clientId) {
    return CLIENT_DATA_SCHEMA.get(clientId);
  }

  /** Get All Device Schema */
  public List<DeviceSchema> getAllDeviceSchemas() {
    return new ArrayList<>(NAME_DATA_SCHEMA.values());
  }

  public static SchemaEngine getInstance() {
    if (schemaEngine == null) {
      synchronized (SchemaEngine.class) {
        if (schemaEngine == null) {
          schemaEngine = new SchemaEngine();
        }
      }
    }
    return schemaEngine;
  }
}
