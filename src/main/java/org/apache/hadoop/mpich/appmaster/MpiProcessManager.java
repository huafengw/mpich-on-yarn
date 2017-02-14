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
package org.apache.hadoop.mpich.appmaster;

import io.netty.channel.Channel;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MpiProcessManager {
  private Map<String, KVStore> kvStores;
  private Map<Channel, MpiProcess> channelToProcess;
  private MpiProcessGroup currentGroup;
  private int numKvStores = 0;

  public MpiProcessManager(List<MpiProcess> processes) {
    this.kvStores = new HashMap<String, KVStore>();
    this.currentGroup = new MpiProcessGroup(processes, newKvStore());
    this.channelToProcess = new HashMap<Channel, MpiProcess>();
  }

  public MpiProcess getProcessById(int pmiid) {
    return currentGroup.getProcessById(pmiid);
  }

  public synchronized void addClient(int pmiid, Channel channel) {
    MpiProcess process = currentGroup.getProcessById(pmiid);
    if (process != null) {
      process.setChannel(channel);
      this.channelToProcess.put(channel, process);
    } else {
      System.err.println("Can not find process by id " + pmiid);
    }
  }

  public KVStore getKvStore(String name) {
    return this.kvStores.get(name);
  }

  public MpiProcess getProcessByChannel(Channel channel) {
    return channelToProcess.get(channel);
  }

  private KVStore newKvStore() {
    String kvsName = "kvs_" + Utils.getPID() + "_" + numKvStores;
    KVStore kvStore = new KVStore(kvsName);
    this.kvStores.put(kvsName, kvStore);
    numKvStores += 1;
    return kvStore;
  }

  public int getUniverseSize() {
    return currentGroup.getNumProcesses();
  }
}