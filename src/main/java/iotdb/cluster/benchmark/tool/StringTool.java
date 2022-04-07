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
