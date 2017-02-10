package org.apache.hadoop.mpich.util;

import org.apache.hadoop.mpich.appmaster.pmi.ClientToServerCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Utils {
  public static int findPort(ServerSocket sock) {
    int minPort = 25000;
    int maxPort = 40000;
    int selectedPort;

    /* The loop generates a random port number, opens a socket on
     * the generated port
     */
    while (true) {
      Random rand = new Random();
      selectedPort = (rand.nextInt((maxPort - minPort) + 1) + minPort);

      try {
        sock.bind(new InetSocketAddress(selectedPort));
      } catch (IOException e) {
        System.err.println("[Utils.java]:- " + selectedPort +
          "]Port already in use. Checking for a new port..");
        continue;
      }
      break;
    }
    return selectedPort;
  }

  public static ClientToServerCommand getCommand(Map<String, String> kvs) {
    String command = kvs.get("cmd");
    if (command != null && !command.equals("")) {
      return ClientToServerCommand.valueOf(command.toUpperCase());
    } else {
      return ClientToServerCommand.UNRECOGNIZED;
    }
  }

  public static Map<String, String> parseKeyVals(String msg) throws Exception {
    Map<String, String> results = new HashMap<String, String>();
    String[] kvPairs = msg.split("\\s+");
    for(String kvPair : kvPairs) {
      String[] kv = kvPair.trim().split("=");
      if (kv.length != 2) {
        throw new Exception("Parse message " + msg + " failed");
      } else {
        results.put(kv[0], kv[1]);
      }
    }
    return results;
  }
}
