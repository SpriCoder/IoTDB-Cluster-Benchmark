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

package iotdb.cluster.benchmark.measurement;

import com.clearspring.analytics.stream.quantile.TDigest;
import iotdb.cluster.benchmark.config.Config;
import iotdb.cluster.benchmark.config.ConfigDescriptor;
import iotdb.cluster.benchmark.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Measurement {
  private static final Logger logger = LoggerFactory.getLogger(Measurement.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Map<Operation, TDigest> operationLatencyDigest =
      new EnumMap<>(Operation.class);
  private static final Map<Operation, Double> operationLatencySumAllClient =
      new EnumMap<>(Operation.class);
  private double elapseTime;
  private final Map<Operation, Double> operationLatencySumThisClient;
  private final Map<Operation, Long> okOperationNumMap;
  private final Map<Operation, Long> failOperationNumMap;
  private final Map<Operation, Long> okPointNumMap;
  private final Map<Operation, Long> failPointNumMap;
  private static final String RESULT_ITEM = "%-25s";
  private static final String LATENCY_ITEM = "%-12s";
  /** Precision = 3 / COMPRESSION */
  private static final int COMPRESSION =
      (int) (300 / config.getGeneralConfig().getResultPrecision());

  static {
    for (Operation operation : Operation.values()) {
      operationLatencyDigest.put(
          operation, new TDigest(COMPRESSION, new Random(config.getGeneralConfig().getDataSeed())));
      operationLatencySumAllClient.put(operation, 0D);
    }
  }

  public Measurement() {
    okOperationNumMap = new EnumMap<>(Operation.class);
    failOperationNumMap = new EnumMap<>(Operation.class);
    okPointNumMap = new EnumMap<>(Operation.class);
    failPointNumMap = new EnumMap<>(Operation.class);
    operationLatencySumThisClient = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, 0L);
      failOperationNumMap.put(operation, 0L);
      okPointNumMap.put(operation, 0L);
      failPointNumMap.put(operation, 0L);
      operationLatencySumThisClient.put(operation, 0D);
    }
  }

  /**
   * Users need to call calculateMetrics() after calling mergeMeasurement() to update metrics.
   *
   * @param m measurement to be merged
   */
  public void mergeMeasurement(Measurement m) {
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(
          operation, okOperationNumMap.get(operation) + m.getOkOperationNum(operation));
      failOperationNumMap.put(
          operation, failOperationNumMap.get(operation) + m.getFailOperationNum(operation));
      okPointNumMap.put(operation, okPointNumMap.get(operation) + m.getOkPointNum(operation));
      failPointNumMap.put(operation, failPointNumMap.get(operation) + m.getFailPointNum(operation));

      // set operationLatencySumThisClient of this measurement the largest latency sum among all
      // threads
      if (operationLatencySumThisClient.get(operation)
          < m.getOperationLatencySumThisClient().get(operation)) {
        operationLatencySumThisClient.put(
            operation, m.getOperationLatencySumThisClient().get(operation));
      }
      operationLatencySumAllClient.put(
          operation,
          operationLatencySumAllClient.get(operation)
              + m.getOperationLatencySumThisClient().get(operation));
    }
  }

  /** Calculate metrics of each operation */
  public void calculateMetrics(List<Operation> operations) {
    for (Operation operation : operations) {
      double avgLatency;
      if (okOperationNumMap.get(operation) != 0) {
        avgLatency = operationLatencySumAllClient.get(operation) / okOperationNumMap.get(operation);
        Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
        Metric.MAX_THREAD_LATENCY_SUM
            .getTypeValueMap()
            .put(operation, operationLatencySumThisClient.get(operation));
        Metric.MIN_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.0));
        Metric.MAX_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(1.0));
        Metric.P10_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.1));
        Metric.P25_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.25));
        Metric.MEDIAN_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.50));
        Metric.P75_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.75));
        Metric.P90_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.90));
        Metric.P95_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.95));
        Metric.P99_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.99));
        Metric.P999_LATENCY
            .getTypeValueMap()
            .put(operation, operationLatencyDigest.get(operation).quantile(0.999));
      }
    }
  }

  /** Show measurements */
  public void showMeasurements(List<Operation> operations) {
    System.out.println(Thread.currentThread().getName() + " measurements:");
    System.out.println("Test elapsed time: " + String.format("%.2f", elapseTime) + " second");

    System.out.println(
        "----------------------------------------------------------Result Matrix----------------------------------------------------------");
    StringBuffer format = new StringBuffer();
    for (int i = 0; i < 6; i++) {
      format.append(RESULT_ITEM);
    }
    format.append("\n");
    System.out.printf(
        format.toString(),
        "Operation",
        "okOperation",
        "okPoint",
        "failOperation",
        "failPoint",
        "throughput(point/s)");
    for (Operation operation : operations) {
      String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
      System.out.printf(
          format.toString(),
          operation.getName(),
          okOperationNumMap.get(operation),
          okPointNumMap.get(operation),
          failOperationNumMap.get(operation),
          failPointNumMap.get(operation),
          throughput);
    }
    System.out.println(
        "---------------------------------------------------------------------------------------------------------------------------------");
  }

  /** Show Config of test */
  public void showConfigs() {
    System.out.println("----------------------Main Configurations----------------------");
    System.out.println(config.getShowConfigProperties().toString());
    System.out.println("---------------------------------------------------------------");
  }

  /** Show metrics of test */
  public void showMetrics(List<Operation> operations) {
    System.out.println(
        "--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------");
    System.out.printf(RESULT_ITEM, "Operation");
    for (Metric metric : Metric.values()) {
      System.out.printf(LATENCY_ITEM, metric.name);
    }
    System.out.println();
    for (Operation operation : operations) {
      System.out.printf(RESULT_ITEM, operation.getName());
      for (Metric metric : Metric.values()) {
        String metricResult = String.format("%.2f", metric.typeValueMap.get(operation));
        System.out.printf(LATENCY_ITEM, metricResult);
      }
      System.out.println();
    }
    System.out.println(
        "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------");
  }

  /** output measurement to csv */
  public void outputCSV() {
    MeasurementCsvWriter measurementCsvWriter = new MeasurementCsvWriter();
    measurementCsvWriter.write();
  }

  /** A class which write measurement to csv file */
  private class MeasurementCsvWriter {
    public void write() {
      try {
        String fileName = createFileName();
        File csv = new File(fileName);
        createDirectory();
        csv.createNewFile();
        outputConfigToCSV(csv);
        outputResultMetricToCSV(csv);
        outputLatencyMetricsToCSV(csv);

      } catch (IOException e) {
        logger.error("Exception occurred during writing csv file because: ", e);
      }
    }

    /** Get filename */
    private String createFileName() {
      // Formatting time
      SimpleDateFormat sdf = new SimpleDateFormat();
      sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
      Date date = new Date();
      String currentTime = sdf.format(date);

      return "data/csvOutput/" + currentTime + "-test-result.csv";
    }

    /** Create directory if not exists */
    private void createDirectory() {
      File folder = new File("data/csvOutput");
      if (!folder.exists() && !folder.isDirectory()) {
        folder.mkdirs();
      }
    }

    /** Write config to csv */
    private void outputConfigToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.write("Main Configurations" + System.lineSeparator());
        bw.write(config.getAllConfigProperties().toString());
        bw.close();
      } catch (IOException e) {
        logger.error("Exception occurred during operating buffer writer because: ", e);
      }
    }

    /** Write result metric to csv */
    private void outputResultMetricToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.newLine();
        bw.write("Result Matrix");
        bw.newLine();
        bw.write(
            "Operation"
                + ","
                + "okOperation"
                + ","
                + "okPoint"
                + ","
                + "failOperation"
                + ","
                + "failPoint"
                + ","
                + "throughput(point/s)");
        for (Operation operation : Operation.values()) {
          String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
          bw.newLine();
          bw.write(
              operation.getName()
                  + ","
                  + okOperationNumMap.get(operation)
                  + ","
                  + okPointNumMap.get(operation)
                  + ","
                  + failOperationNumMap.get(operation)
                  + ","
                  + failPointNumMap.get(operation)
                  + ","
                  + throughput);
        }
        bw.close();
      } catch (IOException e) {
        logger.error("Exception occurred during operating buffer writer because: ", e);
      }
    }

    /** Write Latency metric to csv */
    private void outputLatencyMetricsToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.newLine();
        bw.write("Latency (ms) Matrix");
        bw.newLine();
        bw.write("Operation");
        for (Metric metric : Metric.values()) {
          bw.write("," + metric.name);
        }
        bw.newLine();
        for (Operation operation : Operation.values()) {
          bw.write(operation.getName());
          for (Metric metric : Metric.values()) {
            String metricResult = String.format("%.2f", metric.typeValueMap.get(operation));
            bw.write("," + metricResult);
          }
          bw.newLine();
        }
        bw.close();
      } catch (IOException e) {
        logger.error("Exception occurred during operating buffer writer because: ", e);
      }
    }
  }

  private Map<Operation, Double> getOperationLatencySumThisClient() {
    return operationLatencySumThisClient;
  }

  private long getOkOperationNum(Operation operation) {
    return okOperationNumMap.get(operation);
  }

  private long getFailOperationNum(Operation operation) {
    return failOperationNumMap.get(operation);
  }

  private long getOkPointNum(Operation operation) {
    return okPointNumMap.get(operation);
  }

  private long getFailPointNum(Operation operation) {
    return failPointNumMap.get(operation);
  }

  public void addOperationLatency(Operation op, double latency) {
    synchronized (operationLatencyDigest.get(op)) {
      operationLatencyDigest.get(op).add(latency);
    }
    operationLatencySumThisClient.put(op, operationLatencySumThisClient.get(op) + latency);
  }

  public void addOkPointNum(Operation operation, int pointNum) {
    okPointNumMap.put(operation, okPointNumMap.get(operation) + pointNum);
  }

  public void addFailPointNum(Operation operation, int pointNum) {
    failPointNumMap.put(operation, failPointNumMap.get(operation) + pointNum);
  }

  public void addOkOperationNum(Operation operation) {
    okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
  }

  public void addFailOperationNum(Operation operation) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
  }

  public double getElapseTime() {
    return elapseTime;
  }

  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }
}
