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
  private boolean isSpawn;
  private int numApps;
  private int numProcs;
  private int worldNum;
  private KVStore kvStore;
  private List<ProcessApp> apps;

  public ProcessWorld() {
    this(false);
  }

  public ProcessWorld(boolean isSpawn) {
    this.numApps = 0;
    this.numProcs = 0;
    this.apps = new ArrayList<ProcessApp>();
    this.kvStore = KVStoreFactory.newKVStore();
    this.isSpawn = isSpawn;
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

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ProcessWorld info:\n");
    builder.append("     KVStore name: " + kvStore.getName() + "\n");
    builder.append("     Embedded " + apps.size() + " ProcessApp:" + "\n");
    for (ProcessApp app: apps) {
      builder.append(app.toString());
    }
    return builder.toString();
  }

  public boolean isSpawn() {
    return isSpawn;
  }
}
