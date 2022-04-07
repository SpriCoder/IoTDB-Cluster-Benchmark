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

import java.util.LinkedHashMap;
import java.util.Map;

public class ConfigProperties {
  private Map<String, Map<String, Object>> properties = new LinkedHashMap<>();
  private Map<String, Object> allProperties = new LinkedHashMap<>();

  public void addProperty(String groupName, String name, Object value) {
    if (!properties.containsKey(groupName)) {
      properties.put(groupName, new LinkedHashMap<>());
    }
    properties.get(groupName).put(name, value);
    allProperties.put(name, value);
  }

  public Map<String, Object> getAllProperties() {
    return allProperties;
  }

  @Override
  public String toString() {
    StringBuffer configPropertiesStr = new StringBuffer();
    for (Map.Entry<String, Map<String, Object>> group : properties.entrySet()) {
      configPropertiesStr
          .append("########### ")
          .append(group.getKey())
          .append(" ###########")
          .append(System.lineSeparator());
      for (Map.Entry<String, Object> property : group.getValue().entrySet()) {
        configPropertiesStr
            .append(property.getKey())
            .append("=")
            .append(property.getValue())
            .append(System.lineSeparator());
      }
    }
    return configPropertiesStr.toString();
  }
}
