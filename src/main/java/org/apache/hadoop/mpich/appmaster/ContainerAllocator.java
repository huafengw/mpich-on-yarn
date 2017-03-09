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
import org.apache.hadoop.mpich.MpichContainerWrapper;
import org.apache.hadoop.mpich.util.Constants;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

public class ContainerAllocator {
  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);
  private MpiApplicationContext appContext;
  private AMRMClient<ContainerRequest> amrmClient;
  private NMClient nmClient;

  public ContainerAllocator(MpiApplicationContext context, AMRMClient<ContainerRequest> amrmClient) {
    this.appContext = context;
    this.amrmClient = amrmClient;

    this.nmClient = NMClient.createNMClient();
    this.nmClient.init(appContext.getConf());
    this.nmClient.start();
  }

  public List<Container> allocate(Map<String, Integer> hostToContainerCount)
      throws IOException, YarnException, InterruptedException {
    int containerNum = 0;
    for (String host : hostToContainerCount.keySet()) {
      Integer num = hostToContainerCount.get(host);
      LOG.info("Requesting " + num + " containers on host " + host);
      for (int i = 0; i < num; i++) {
        String[] hosts = null;
        if (!host.equals(Constants.ANY_HOST)) {
          // Todo: standardize the hosts format
          hosts = new String[]{host};
        }
        ContainerRequest request = new ContainerRequest(appContext.getContainerResource(),
          hosts, null, appContext.getContainerPriority());
        this.amrmClient.addContainerRequest(request);
        containerNum += 1;
      }
    }

    List<Container> allocated = new ArrayList<Container>();
    while (allocated.size() < containerNum) {
      AllocateResponse response = amrmClient.allocate(0);
      allocated.addAll(response.getAllocatedContainers());

      if (allocated.size() != containerNum) {
        Thread.sleep(100);
      }
    }

    return allocated;
  }

  public void removeMatchingRequest(Container allocatedContainer) {
    // Certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    Resource matchingResource = Resource.newInstance(allocatedContainer.getResource().getMemory(),
      this.appContext.getContainerResource().getVirtualCores());
    List<? extends Collection<ContainerRequest>> matchingRequests = amrmClient.getMatchingRequests(
      allocatedContainer.getPriority(), allocatedContainer.getNodeId().getHost(), matchingResource);
    if (!matchingRequests.isEmpty()) {
      ContainerRequest request = matchingRequests.get(0).iterator().next();
      amrmClient.removeContainerRequest(request);
    } else {
      // LOG ERROR
    }
  }

  public void launchContainer(Container container, MpiProcess mpiProcess)
      throws IOException, YarnException {
    LOG.info("Container " + container.getId() + " will launch MpiProcess:\n");
    LOG.info(mpiProcess.toString());
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    List<String> commands = new ArrayList<String>();

    commands.add("$JAVA_HOME/bin/java");
    commands.add("-Xmx" + appContext.getContainerResource().getMemory() + "m");
    commands.add(MpichContainerWrapper.class.getName());
    commands.add("--ioServer");
    commands.add(appContext.getIoServer());          // server name
    commands.add("--ioServerPort");
    commands.add(Integer.toString(appContext.getIoServerPort())); // IO server port

    commands.add("--pmiServer");
    commands.add(appContext.getPmiServer());
    commands.add("--pmiServerPort");
    commands.add(String.valueOf(appContext.getPmiServerPort()));

    commands.addAll(getMpiSpecificCommands(mpiProcess));

    commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    ctx.setCommands(commands);
    ctx.setLocalResources(appContext.getLocalResources());

    Map<String, String> containerEnv = new HashMap<String, String>();
    setupEnv(containerEnv);
    ctx.setEnvironment(containerEnv);

    nmClient.startContainer(container, ctx);
  }

  private List<String> getMpiSpecificCommands(MpiProcess process) {
    List<String> commands = new ArrayList<String>();
    commands.add("--exec");
    commands.add(process.getApp().getExeName());
    commands.add("--np");
    commands.add(Integer.toString(process.getGroup().getNumProcesses()));
    commands.add("--rank");
    commands.add(Integer.toString(process.getRank()));
    commands.add("--pmiid");
    commands.add(Integer.toString(process.getPmiid()));
    if (process.isSpawn()) {
      commands.add("--isSpawn");
    }
    List<String> appArgs = process.getApp().getArgs();
    if (appArgs != null & appArgs.size() > 0) {
      commands.add("--appArgs");
      commands.addAll(appArgs);
    }
    return commands;
  }

  private void setupEnv(Map<String, String> containerEnv) {
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
      .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : appContext.getConf().getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }

    containerEnv.put("CLASSPATH", classPathEnv.toString());
  }

  public void stop() {
    this.nmClient.stop();
  }
}
