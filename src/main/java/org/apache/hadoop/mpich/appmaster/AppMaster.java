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
import org.apache.hadoop.mpich.appmaster.netty.PMIServer;
import org.apache.hadoop.mpich.util.Utils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppMaster {
  private Configuration conf;
  private String pmServerHost;
  private int pmServerPort;
  private Socket ioServerSock;
  private String wrapperPath;
  private PMIServer pmiServer;
  private int maxMem;
  private int maxCores;
  private int allocatedContainers;
  private int completedContainers;
  private AppMasterArguments appArguments;
  private AMRMClient<ContainerRequest> rmClient;
  private MpiProcessManager mpiProcessManager;
  private List<Container> mpiContainers = new ArrayList<Container>();

  public AppMaster() {
    conf = new YarnConfiguration();
  }

  public void init(String[] args) {
    try {
      this.appArguments = AppMasterArgumentsParser.parse(args);
      pmServerHost = InetAddress.getLocalHost().getHostName();
      pmServerPort = Utils.findFreePort();

      // Initialize AM <--> RM communication protocol
      rmClient = AMRMClient.createAMRMClient();
      rmClient.init(conf);
      rmClient.start();

      MpiApplicationContext applicationContext = null;
      ContainerAllocator allocator = new ContainerAllocator(applicationContext, this.rmClient);
      this.mpiProcessManager = new MpiProcessManager(allocator);
      this.pmiServer = new PMIServer(mpiProcessManager, pmServerPort);

    } catch (Exception exp) {
      exp.printStackTrace();
    }
  }

  public void run() throws Exception {
    try {
      ioServerSock = new Socket(appArguments.getIoServer(), appArguments.getIoServerPort());

      //redirecting stdout and stderr
      System.setOut(new PrintStream(ioServerSock.getOutputStream(), true));
      System.setErr(new PrintStream(ioServerSock.getOutputStream(), true));
    } catch (Exception exp) {
      exp.printStackTrace();
    }

    FileSystem fs = FileSystem.get(conf);
    Path wrapperDest = new Path(wrapperPath);
    FileStatus destStatus = fs.getFileStatus(wrapperDest);

    // Initialize AM <--> NM communication protocol
    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    RegisterApplicationMasterResponse registerResponse =
      rmClient.registerApplicationMaster("", 0, "");
    // Priority for containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(appArguments.getMpjContainerPriority());

    maxMem = registerResponse.getMaximumResourceCapability().getMemory();

    if (appArguments.isDebugYarn()) {
      System.out.println("[MPJAppMaster]: Max memory capability resources " +
        "in cluster: " + maxMem);
    }

//    if (containerMem > maxMem) {
//      System.out.println("[MPJAppMaster]: container  memory specified above " +
//        "threshold of cluster! Using maximum memory for " +
//        "containers: " + containerMem);
//      containerMem = maxMem;
//    }

    maxCores = registerResponse.getMaximumResourceCapability().getVirtualCores();

    if (appArguments.isDebugYarn()) {
      System.out.println("[MPJAppMaster]: Max v-cores capability resources " +
        "in cluster: " + maxCores);
    }

//    if (containerCores > maxCores) {
//      System.out.println("[MPJAppMaster]: virtual cores specified above " +
//        "threshold of cluster! Using maximum v-cores for " +
//        "containers: " + containerCores);
//      containerCores = maxCores;
//    }

    // Resource requirements for containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(appArguments.getContainerMem());
    capability.setVirtualCores(appArguments.getContainerCores());

    // Make container requests to ResourceManager
    for (int i = 0; i < appArguments.getNp(); ++i) {
      ContainerRequest containerReq =
        new ContainerRequest(capability, null, null, priority);

      rmClient.addContainerRequest(containerReq);
    }

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    // Creating Local Resource for Wrapper
    LocalResource wrapperJar = Records.newRecord(LocalResource.class);

    wrapperJar.setResource(ConverterUtils.getYarnUrlFromPath(wrapperDest));
    wrapperJar.setSize(destStatus.getLen());
    wrapperJar.setTimestamp(destStatus.getModificationTime());
    wrapperJar.setType(LocalResourceType.ARCHIVE);
    wrapperJar.setVisibility(LocalResourceVisibility.APPLICATION);

    // Creating Local Resource for UserClass
    localResources.put("mpj-yarn-wrapper.jar", wrapperJar);

    while (allocatedContainers < appArguments.getNp()) {
      AllocateResponse response = rmClient.allocate(0);
      mpiContainers.addAll(response.getAllocatedContainers());
      allocatedContainers = mpiContainers.size();

      if (allocatedContainers != appArguments.getNp()) {
        Thread.sleep(100);
      }
    }

    if (appArguments.isDebugYarn()) {
      System.out.println("[MPJAppMaster]: launching " + allocatedContainers +
        " containers");
    }

    try {
      this.pmiServer = new PMIServer(mpiProcessManager, 0);
      pmiServer.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (Container container : mpiContainers) {
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

      List<String> commands = new ArrayList<String>();

      commands.add(" $JAVA_HOME/bin/java");
      commands.add(" -Xmx" + appArguments.getContainerMem() + "m");
      commands.add(" runtime.starter.MpichYarnWrapper");
      commands.add("--ioServer");
      commands.add(appArguments.getIoServer());          // server name
      commands.add("--ioServerPort");
      commands.add(Integer.toString(appArguments.getIoServerPort())); // IO server port
      commands.add("--psl");
      commands.add(appArguments.getPsl());                 // protocol switch limit

      //temp sock port to share rank and ports
      commands.add("--pmiServer");
      commands.add(pmServerHost);
      commands.add("--pmiServerPort");
      commands.add(String.valueOf(pmServerPort));

      if (appArguments.getAppArgs() != null) {
        commands.add("--appArgs");
        for (int i = 0; i < appArguments.getAppArgs().length; i++) {
          commands.add(appArguments.getAppArgs()[i]);
        }
      }

      ctx.setCommands(commands);

      // Set local resource for containers
      ctx.setLocalResources(localResources);

      // Set environment for container
      Map<String, String> containerEnv = new HashMap<String, String>();
      setupEnv(containerEnv);
      ctx.setEnvironment(containerEnv);

      // Time to acceptAll the container
      nmClient.startContainer(container, ctx);
    }

    while (completedContainers < appArguments.getNp()) {
      // argument to allocate() is the progress indicator
      AllocateResponse response = rmClient.allocate(completedContainers / appArguments.getNp());

      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        if (appArguments.isDebugYarn()) {
          System.out.println("\n[MPJAppMaster]: Container Id - " +
            status.getContainerId());
          System.out.println("[MPJAppMaster]: Container State - " +
            status.getState().toString());
          System.out.println("[MPJAppMaster]: Container Diagnostics - " +
            status.getDiagnostics());

        }
        ++completedContainers;
      }

      if (completedContainers != appArguments.getNp()) {
        Thread.sleep(100);
      }
    }
    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,"", "");
    //shutDown AppMaster IO
    System.out.println("EXIT");
  }

  private void setupEnv(Map<String, String> containerEnv) {
    for (String c : conf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {

      Apps.addToEnvironment(containerEnv, Environment.CLASSPATH.name(), c.trim());
    }

    Apps.addToEnvironment(containerEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*");
  }

  public static void main(String[] args) throws Exception {
    for (String x : args) {
      System.out.println(x);
    }
    AppMaster am = new AppMaster();
    am.init(args);
    am.run();
  }
}
