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

import org.apache.hadoop.mpich.ProcessWorld;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;

import java.util.List;
import java.util.Map;

public class ContainerLauncher implements MpiProcessWorldLauncher {
  private MpiApplicationContext context;
  private AMRMClient<ContainerRequest> amrmClient;
  private NMClient nmClient;

  public ContainerLauncher(MpiApplicationContext context) {
    this.context = context;
    this.amrmClient = AMRMClient.createAMRMClient();
    this.nmClient = NMClient.createNMClient();
  }

  public void init() {

  }

  private ContainerRequest createContainerRequest(Resource resource, String[] hosts,
      String[] racks, Priority priority) {
    return new ContainerRequest(resource, hosts, racks, priority);
  }

  public List<Container> allocate(int totalRequest, Map<String, Integer> hostToContainerCount) {
    return null;
  }

  public boolean launch(ProcessWorld processWorld) {

    return true;
  }
}
