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

import org.apache.hadoop.mpich.MpiProcess;
import org.apache.hadoop.mpich.ProcessApp;
import org.apache.hadoop.mpich.ProcessWorld;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PendingMpiProcesses {
  private ProcessWorld processWorld;
  private Map<String, Queue<MpiProcess>> remainingProcesses;
  private int rank = 0;

  public PendingMpiProcesses(ProcessWorld processWorld) {
    this.processWorld = processWorld;
    this.remainingProcesses = new HashMap<String, Queue<MpiProcess>>();
    for (ProcessApp app: processWorld.getApps()) {
      Queue<MpiProcess> mpiProcesses = this.remainingProcesses.get(app.getHostName());
      if (mpiProcesses == null) {
        mpiProcesses = new ConcurrentLinkedQueue<MpiProcess>();
        this.remainingProcesses.put(app.getHostName(), mpiProcesses);
      }
      for (int i = 0; i < app.getNumProcess(); i++) {
        MpiProcess process = new MpiProcess(rank, UniqueId.nextId(), app.getHostName(), app);
        mpiProcesses.add(process);
        this.rank += 1;
      }
    }
  }

  public int remainingProcNum() {
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
