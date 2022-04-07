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

package iotdb.cluster.benchmark.tool;

import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;

import java.util.Random;

public class StringTool {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Random random = new Random(config.getGeneralConfig().getDataSeed());

  public static String getRandomIp() {
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int length = str.length();
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < config.getDataNodeConfig().getIpLength(); i++) {
      int number = random.nextInt(length);
      sb.append(str.charAt(number));
    }
    return sb.toString();
  }
}
