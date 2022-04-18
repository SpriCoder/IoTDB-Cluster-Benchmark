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

import iotdb.cluster.benchmark.tool.MetaTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DeviceSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  /** Each device belongs to one storage group */
  private String group;
  /** Name of device, e.g. prefix + deviceId */
  private String device;

  public DeviceSchema(int deviceId) {
    this.device = MetaTool.getDeviceName(deviceId);
    int thisDeviceStorageGroupIndex = MetaTool.calculateStorageGroupId(deviceId);
    this.group = MetaTool.getGroupName(thisDeviceStorageGroupIndex);
  }

  public String getGroup() {
    return group;
  }

  public String getDevice() {
    return device;
  }

  @Override
  public String toString() {
    return "DeviceSchema{" + "group='" + group + '\'' + ", device='" + device + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeviceSchema that = (DeviceSchema) o;
    return Objects.equals(group, that.group) && Objects.equals(device, that.device);
  }

  @Override
  public int hashCode() {
    return Objects.hash(group, device);
  }
}
