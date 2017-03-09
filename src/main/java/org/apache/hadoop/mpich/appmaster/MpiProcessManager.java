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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mpich.MpiProcess;
import org.apache.hadoop.mpich.MpiProcessGroup;
import org.apache.hadoop.mpich.ProcessWorld;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.yarn.api.records.Container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MpiProcessManager implements MpiProcessWorldLauncher {
  private static final Log LOG = LogFactory.getLog(MpiProcessManager.class);
  private Map<String, KVStore> kvStores;
  private Map<Integer, MpiProcess> pmiidToProcess;
  private List<MpiProcess> finishedProcess;
  private ContainerAllocator containerAllocator;

  public MpiProcessManager(ContainerAllocator containerAllocator) {
    this.kvStores = new HashMap<String, KVStore>();
    this.pmiidToProcess = new HashMap<Integer, MpiProcess>();
    this.finishedProcess = new ArrayList<MpiProcess>();
    this.containerAllocator = containerAllocator;
  }

  public synchronized void addMpiProcessGroup(MpiProcessGroup mpiProcessGroup) {
    this.kvStores.put(mpiProcessGroup.getKvStore().getName(), mpiProcessGroup.getKvStore());
    for (MpiProcess process : mpiProcessGroup.getProcesses()) {
      this.pmiidToProcess.put(process.getPmiid(), process);
    }
  }

  public MpiProcess getProcessById(int pmiid) {
    return this.pmiidToProcess.get(pmiid);
  }

  public KVStore getKvStore(String name) {
    return this.kvStores.get(name);
  }

  public int getUniverseSize() {
    return this.pmiidToProcess.size();
  }

  public synchronized boolean launch(ProcessWorld processWorld) {
    LOG.info("Launching ProcessWorld:\n" + processWorld.toString());
    try {
      MpiProcessGroup group = new MpiProcessGroup(processWorld, processWorld.getKvStore());
      List<Container> containers = this.containerAllocator.allocate(group.getHostProcMap());
      LOG.info("Allocated " + containers.size() + " containers");

      for (Container container : containers) {
        String host = container.getNodeId().getHost();
        MpiProcess processToLaunch = group.getNextProcessToLaunch(host);
        if (processToLaunch != null) {
          this.containerAllocator.launchContainer(container, processToLaunch);
          this.containerAllocator.removeMatchingRequest(container);
        } else {
          LOG.error("Failed to find matching process on host " + host);
        }
      }
      this.addMpiProcessGroup(group);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public synchronized void processFinished(MpiProcess mpiProcess) {
    this.finishedProcess.add(mpiProcess);
  }

  public boolean allAppFinished() {
    return this.finishedProcess.size() == this.getUniverseSize();
  }

  public void close() {
    this.containerAllocator.stop();
  }
}
