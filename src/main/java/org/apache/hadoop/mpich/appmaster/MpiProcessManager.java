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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mpich.MpiProcess;
import org.apache.hadoop.mpich.MpiProcessGroup;
import org.apache.hadoop.mpich.ProcessWorld;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.PendingMpiProcesses;
import org.apache.hadoop.yarn.api.records.Container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MpiProcessManager implements MpiProcessWorldLauncher {
  private static final Log LOG = LogFactory.getLog(MpiProcessManager.class);
  private Map<String, KVStore> kvStores;
  private Map<Integer, MpiProcess> pmiidToProcess;
  private Map<Channel, MpiProcess> channelToProcess;
  private ContainerAllocator containerAllocator;

  // For test
  public MpiProcessManager() {
    this(null);
  }

  public MpiProcessManager(ContainerAllocator containerAllocator) {
    this.kvStores = new HashMap<String, KVStore>();
    this.pmiidToProcess = new HashMap<Integer, MpiProcess>();
    this.channelToProcess = new HashMap<Channel, MpiProcess>();
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

  public synchronized void addClient(int pmiid, Channel channel) {
    MpiProcess process = this.getProcessById(pmiid);
    if (process != null) {
      process.setChannel(channel);
      this.channelToProcess.put(channel, process);
    } else {
      LOG.error("Can not find process by id " + pmiid);
    }
  }

  public KVStore getKvStore(String name) {
    return this.kvStores.get(name);
  }

  public MpiProcess getProcessByChannel(Channel channel) {
    return channelToProcess.get(channel);
  }

  public int getUniverseSize() {
    return this.pmiidToProcess.size();
  }

  public synchronized boolean launch(ProcessWorld processWorld) {
    try {
      PendingMpiProcesses pendingMpiProcesses = new PendingMpiProcesses(processWorld);
      List<Container> containers = this.containerAllocator.allocate(pendingMpiProcesses.getHostProcMap());
      assert containers.size() == pendingMpiProcesses.remainingProcNum();

      List<MpiProcess> launched = new ArrayList<MpiProcess>();
      for (Container container : containers) {
        String host = container.getNodeId().getHost();
        MpiProcess processToLaunch = pendingMpiProcesses.getNextProcessToLaunch(host);
        if (processToLaunch != null) {
          this.containerAllocator.launchContainer(container, processToLaunch);
          launched.add(processToLaunch);
          this.containerAllocator.removeMatchingRequest(container);
        } else {
          LOG.error("Failed to find matching process on host " + host);
        }
      }
      MpiProcessGroup group = new MpiProcessGroup(launched, processWorld.getKvStore());
      this.addMpiProcessGroup(group);
      return true;
    } catch (Exception e) {
      // LOG ERROR
    }
    return false;
  }
}
