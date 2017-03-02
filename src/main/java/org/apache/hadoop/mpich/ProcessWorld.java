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
package org.apache.hadoop.mpich;

import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.KVStoreFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessWorld {
  private int numApps;
  private int numProcs;
  private int worldNum;
  private KVStore kvStore;
  private List<ProcessApp> apps;

  public ProcessWorld() {
    this.numApps = 0;
    this.numProcs = 0;
    this.apps = new ArrayList<ProcessApp>();
    this.kvStore = KVStoreFactory.newKVStore();
  }

  public void addProcessApp(ProcessApp app) {
    this.apps.add(app);
    this.numApps += 1;
    this.numProcs += app.getNumProcess();
  }

  public List<ProcessApp> getApps() {
    return this.apps;
  }

  public int getNumProcs() {
    return this.numProcs;
  }

  public KVStore getKvStore() {
    return kvStore;
  }

  public Map<String, Integer> getHostProcMap() {
    Map<String, Integer> hostToProcs = new HashMap<String, Integer>();
    for (ProcessApp app: this.getApps()) {
      String appHost = app.getHostName();
      if (appHost != null && !appHost.equals("")) {
        if (hostToProcs.containsKey(appHost)) {
          hostToProcs.put(appHost, hostToProcs.get(appHost) + app.getNumProcess());
        } else {
          hostToProcs.put(appHost, app.getNumProcess());
        }
      }
    }
    return hostToProcs;
  }
}
