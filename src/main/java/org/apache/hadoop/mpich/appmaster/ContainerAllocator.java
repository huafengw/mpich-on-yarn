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

import org.apache.hadoop.mpich.util.Constants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;

public class ContainerAllocator {
  private MpiApplicationContext context;
  private AMRMClient<ContainerRequest> amrmClient;
  private NMClient nmClient;

  public ContainerAllocator(MpiApplicationContext context, AMRMClient<ContainerRequest> amrmClient) {
    this.context = context;
    this.amrmClient = amrmClient;
    this.nmClient = NMClient.createNMClient();
  }

  public void init() {
    this.amrmClient.start();
    this.nmClient.start();
  }

  private ContainerRequest createContainerRequest(Resource resource, String[] hosts,
      String[] racks, Priority priority) {
    return new ContainerRequest(resource, hosts, racks, priority);
  }

  public List<Container> allocate(Map<String, Integer> hostToContainerCount) throws IOException, YarnException, InterruptedException {
    int requested = 0;
    for (String host : hostToContainerCount.keySet()) {
      Integer num = hostToContainerCount.get(host);
      for (int i = 0; i < num; i++) {
        String[] hosts = null;
        if (!host.equals(Constants.ANY_HOST)) {
          hosts = new String[]{host};
        }
        ContainerRequest request = new ContainerRequest(context.getContainerResource(),
          hosts, null, context.getPriority());
        this.amrmClient.addContainerRequest(request);
        requested += 1;
      }
    }

    int allocatedNum = 0;
    List<Container> containers = new ArrayList<Container>();
//    while (allocatedNum < totalRequest) {
      AllocateResponse response = amrmClient.allocate(0);
      containers.addAll(response.getAllocatedContainers());
      allocatedNum = containers.size();

//      if (allocatedNum != totalRequest) {
//        Thread.sleep(100);
//      }
//    }

    return containers;
  }

  public void removeMatchingRequest(Container allocatedContainer) {
    // Certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    Resource matchingResource = Resource.newInstance(allocatedContainer.getResource().getMemory(),
      this.context.getContainerResource().getVirtualCores());
    List<? extends Collection<ContainerRequest>> matchingRequests = amrmClient.getMatchingRequests(
      allocatedContainer.getPriority(), allocatedContainer.getNodeId().getHost(), matchingResource);
    if (!matchingRequests.isEmpty()) {
      ContainerRequest request = matchingRequests.get(0).iterator().next();
      amrmClient.removeContainerRequest(request);
    } else {
      // LOG ERROR
    }
  }

  public void stop() {
    this.nmClient.stop();
  }
}
