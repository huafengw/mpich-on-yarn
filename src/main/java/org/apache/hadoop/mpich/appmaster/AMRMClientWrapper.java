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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mpich.util.Constants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AMRMClientWrapper {
  private Configuration conf;
  private AppMasterArguments arguments;
  private AMRMClient<AMRMClient.ContainerRequest> rmClient;

  public AMRMClientWrapper(Configuration conf, AppMasterArguments arguments) {
    this.conf = conf;
    this.arguments = arguments;
  }

  public ContainerAllocator register(String pmiServer, int pmiServerPort)
      throws IOException, YarnException {
    rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    RegisterApplicationMasterResponse registerResponse =
      rmClient.registerApplicationMaster("", 0, "");

    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(arguments.getMpjContainerPriority());
    Resource resource = getRevisedResource(registerResponse);
    Map<String, LocalResource> localResource = getLocalResource();

    MpiApplicationContext appContext = new MpiApplicationContext(
      arguments.getIoServer(),
      arguments.getIoServerPort(),
      pmiServer,
      pmiServerPort,
      conf,
      resource,
      priority,
      localResource
    );

    return new ContainerAllocator(appContext, rmClient);
  }

  private Resource getRevisedResource(RegisterApplicationMasterResponse registerResponse) {
    Resource capability = Records.newRecord(Resource.class);
    int containerMem = arguments.getContainerMem();
    int containerCores = arguments.getContainerCores();

    int maxMem = registerResponse.getMaximumResourceCapability().getMemory();
    if (arguments.isDebugYarn()) {
      System.out.println("[MPJAppMaster]: Max memory capability resources " +
        "in cluster: " + maxMem);
    }
    if (containerMem > maxMem) {
      System.out.println("[MPJAppMaster]: container  memory specified above " +
        "threshold of cluster! Using maximum memory for " +
        "containers: " + containerMem);
      containerMem = maxMem;
    }

    int maxCores = registerResponse.getMaximumResourceCapability().getVirtualCores();
    if (arguments.isDebugYarn()) {
      System.out.println("[MPJAppMaster]: Max v-cores capability resources " +
        "in cluster: " + maxCores);
    }
    if (containerCores > maxCores) {
      System.out.println("[MPJAppMaster]: virtual cores specified above " +
        "threshold of cluster! Using maximum v-cores for " +
        "containers: " + containerCores);
      containerCores = maxCores;
    }

    capability.setMemory(containerMem);
    capability.setVirtualCores(containerCores);
    return capability;
  }

  private Map<String, LocalResource> getLocalResource() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Map<String, String> envs = System.getenv();
    assert (envs.containsKey(Constants.APP_JAR_LOCATION));
    String hdfsAppJarLocation = envs.get(Constants.APP_JAR_LOCATION);
    Path wrapperDest = new Path(hdfsAppJarLocation);
    FileStatus destStatus = fs.getFileStatus(wrapperDest);

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LocalResource wrapperJar = Records.newRecord(LocalResource.class);

    wrapperJar.setResource(ConverterUtils.getYarnUrlFromPath(wrapperDest));
    wrapperJar.setSize(destStatus.getLen());
    wrapperJar.setTimestamp(destStatus.getModificationTime());
    wrapperJar.setType(LocalResourceType.FILE);
    wrapperJar.setVisibility(LocalResourceVisibility.APPLICATION);

    // Todo: extract the local resource key string
    localResources.put("mpj-yarn-wrapper.jar", wrapperJar);
    return localResources;
  }

  public void unregister() throws IOException, YarnException {
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,"", "");
  }
}
