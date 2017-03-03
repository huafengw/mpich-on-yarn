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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mpich.util;

import org.apache.hadoop.mpich.appmaster.pmi.ClientToServerCommand;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class Utils {
  public static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    try {
      return socket.getLocalPort();
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
      }
    }
  }

  public static ClientToServerCommand getCommand(Map<String, String> kvs) {
    String command = kvs.get("cmd");
    if (command != null && !command.equals("")) {
      return ClientToServerCommand.valueOf(command.toUpperCase());
    } else {
      String mcmd = kvs.get("mcmd");
      if (mcmd != null && !mcmd.equals("")) {
        return ClientToServerCommand.valueOf(mcmd.toUpperCase());
      }
    }
    return ClientToServerCommand.UNRECOGNIZED;
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

  public static long getPID() {
    String processName = ManagementFactory.getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }
}
