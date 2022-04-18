package iotdb.cluster.benchmark.client.confignode;

import iotdb.cluster.benchmark.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class PartitionClient extends ConfigNodeClient {
  private static final Logger logger = LoggerFactory.getLogger(PartitionClient.class);

  public PartitionClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected boolean doOperation(Operation operation) {
    // TODO do partition test
    // TODO schema related please see SchemaEngine
    return false;
  }
}
