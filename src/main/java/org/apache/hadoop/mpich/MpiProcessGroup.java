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

import org.apache.hadoop.mpich.util.Constants;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.UniqueId;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MpiProcessGroup {
  private final ProcessWorld processWorld;
  private final KVStore kvStore;
//  private int groupId;
  private AtomicInteger numInBarrier;
  private Map<Integer, MpiProcess> idToProcesses;
  private Map<String, Queue<MpiProcess>> remainingProcesses;
  private int rank = 0;

  public MpiProcessGroup(ProcessWorld processWorld, KVStore kvStore) {
    this.processWorld = processWorld;
    this.numInBarrier = new AtomicInteger(0);
    this.remainingProcesses = new HashMap<String, Queue<MpiProcess>>();
    this.idToProcesses = new HashMap<Integer, MpiProcess>();
    this.kvStore = kvStore;
    initPendingProcessQueue();
  }

  private void initPendingProcessQueue() {
    for (ProcessApp app: this.processWorld.getApps()) {
      Queue<MpiProcess> processesQueue = this.remainingProcesses.get(app.getHostName());
      if (processesQueue == null) {
        processesQueue = new ConcurrentLinkedQueue<MpiProcess>();
        this.remainingProcesses.put(app.getHostName(), processesQueue);
      }
      for (int i = 0; i < app.getNumProcess(); i++) {
        MpiProcess process = new MpiProcess(rank, UniqueId.nextId(), app.getHostName(), app, this);
        processesQueue.add(process);
        this.idToProcesses.put(process.getPmiid(), process);
        this.rank += 1;
      }
    }
  }

  public int getNumProcesses() {
    return this.rank;
  }

  public KVStore getKvStore() {
    return kvStore;
  }

  public AtomicInteger getNumInBarrier() {
    return numInBarrier;
  }

  public List<MpiProcess> getProcesses() {
    return new ArrayList<MpiProcess>(this.idToProcesses.values());
  }

  public int pendingProcNum() {
    int remainingNum = 0;
    for (Queue<MpiProcess> queue : this.remainingProcesses.values()) {
      remainingNum += queue.size();
    }
    return remainingNum;
  }

  public Map<String, Integer> getHostProcMap() {
    Map<String, Integer> hostToProcs = new HashMap<String, Integer>();
    for (String host : this.remainingProcesses.keySet()) {
      hostToProcs.put(host, this.remainingProcesses.get(host).size());
    }
    return hostToProcs;
  }

  public MpiProcess getNextProcessToLaunch(String host) {
    Queue<MpiProcess> mpiProcesses = this.remainingProcesses.get(host);
    if (mpiProcesses != null && mpiProcesses.size() > 0) {
      return mpiProcesses.poll();
    } else {
      mpiProcesses = this.remainingProcesses.get(Constants.ANY_HOST);
      if (mpiProcesses != null && mpiProcesses.size() > 0) {
        return mpiProcesses.poll();
      }
    }
    return null;
  }
}
