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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mpich.appmaster.pmi.PMIServer;
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

public class MpichAppMaster {
  private Configuration conf;
  private String localhost;
  private Socket ioServerSock;
  private int rank;
  private Options opts;
  private CommandLine cliParser;
  private int np;
  private String ioServer;
  private int ioServerPort;
  private String wdir;
  private String psl;
  private String wrapperPath;
  private String[] appArgs;
  private PMIServer pmiServer;
  private int currentRank = 0;
  private boolean debugYarn = false;
  private int containerMem;
  private int maxMem;
  private int containerCores;
  private int maxCores;
  private int mpjContainerPriority;
  private int allocatedContainers;
  private int completedContainers;
  private List<Container> mpiContainers = new ArrayList<Container>();

  public MpichAppMaster() {
    conf = new YarnConfiguration();
    opts = new Options();

    opts.addOption("np", true, "Number of Processes");
    opts.addOption("ioServer", true, "Hostname required for Server Socket");
    opts.addOption("ioServerPort", true, "Port required for a socket" +
      " redirecting IO");
    opts.addOption("wdir", true, "Specifies the current working directory");
    opts.addOption("psl", true, "Specifies the Protocol Switch Limit");
    opts.addOption("appArgs", true, "Specifies the User Application args");
    opts.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
    opts.addOption("containerMem", true, "Specifies mpj containers memory");
    opts.addOption("containerCores", true, "Specifies mpj containers v-cores");
    opts.addOption("mpjContainerPriority", true, "Specifies the prioirty of" +
      "containers running MPI processes");
    opts.addOption("debugYarn", false, "Specifies the debug flag");
  }

  public void init(String[] args) {
    try {
      cliParser = new GnuParser().parse(opts, args);

      localhost = InetAddress.getLocalHost().getHostName();
      np = Integer.parseInt(cliParser.getOptionValue("np"));
      ioServer = cliParser.getOptionValue("ioServer");
      ioServerPort = Integer.parseInt(cliParser.getOptionValue("ioServerPort"));
      wdir = cliParser.getOptionValue("wdir");
      psl = cliParser.getOptionValue("psl");

      containerMem = Integer.parseInt(cliParser.getOptionValue
        ("containerMem", "1024"));

      containerCores = Integer.parseInt(cliParser.getOptionValue
        ("containerCores", "1"));

      mpjContainerPriority = Integer.parseInt(cliParser.getOptionValue
        ("mpjContainerPriority", "0"));

      if (cliParser.hasOption("appArgs")) {
        appArgs = cliParser.getOptionValues("appArgs");
      }

      if (cliParser.hasOption("debugYarn")) {
        debugYarn = true;
      }
    } catch (Exception exp) {
      exp.printStackTrace();
    }
  }

  public void run() throws Exception {
    try {
      ioServerSock = new Socket(ioServer, ioServerPort);

      //redirecting stdout and stderr
      System.setOut(new PrintStream(ioServerSock.getOutputStream(), true));
      System.setErr(new PrintStream(ioServerSock.getOutputStream(), true));
    } catch (Exception exp) {
      exp.printStackTrace();
    }

    FileSystem fs = FileSystem.get(conf);
    Path wrapperDest = new Path(wrapperPath);
    FileStatus destStatus = fs.getFileStatus(wrapperDest);

    // Initialize AM <--> RM communication protocol
    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    // Initialize AM <--> NM communication protocol
    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    RegisterApplicationMasterResponse registerResponse =
      rmClient.registerApplicationMaster("", 0, "");
    // Priority for containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(mpjContainerPriority);

    maxMem = registerResponse.getMaximumResourceCapability().getMemory();

    if (debugYarn) {
      System.out.println("[MPJAppMaster]: Max memory capability resources " +
        "in cluster: " + maxMem);
    }

    if (containerMem > maxMem) {
      System.out.println("[MPJAppMaster]: container  memory specified above " +
        "threshold of cluster! Using maximum memory for " +
        "containers: " + containerMem);
      containerMem = maxMem;
    }

    maxCores = registerResponse.getMaximumResourceCapability().getVirtualCores();

    if (debugYarn) {
      System.out.println("[MPJAppMaster]: Max v-cores capability resources " +
        "in cluster: " + maxCores);
    }

    if (containerCores > maxCores) {
      System.out.println("[MPJAppMaster]: virtual cores specified above " +
        "threshold of cluster! Using maximum v-cores for " +
        "containers: " + containerCores);
      containerCores = maxCores;
    }

    // Resource requirements for containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMem);
    capability.setVirtualCores(containerCores);

    // Make container requests to ResourceManager
    for (int i = 0; i < np; ++i) {
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

    while (allocatedContainers < np) {
      AllocateResponse response = rmClient.allocate(0);
      mpiContainers.addAll(response.getAllocatedContainers());
      allocatedContainers = mpiContainers.size();

      if (allocatedContainers != np) {
        Thread.sleep(100);
      }
    }

    if (debugYarn) {
      System.out.println("[MPJAppMaster]: launching " + allocatedContainers +
        " containers");
    }

    List<MpiProcess> mpiProcesses = new ArrayList<MpiProcess>();
    for(Container container: mpiContainers) {
      String host = container.getNodeHttpAddress();
      //Todo: split the rank and pmiid
      MpiProcess process = new MpiProcess(this.currentRank, this.currentRank, host);
      mpiProcesses.add(process);
    }

    try {
      this.pmiServer = new PMIServer(mpiProcesses);
      pmiServer.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (Container container : mpiContainers) {
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

      List<String> commands = new ArrayList<String>();

      commands.add(" $JAVA_HOME/bin/java");
      commands.add(" -Xmx" + containerMem + "m");
      commands.add(" runtime.starter.MpichYarnWrapper");
      commands.add("--ioServer");
      commands.add(ioServer);          // server name
      commands.add("--ioServerPort");
      commands.add(Integer.toString(ioServerPort)); // IO server port
      commands.add("--psl");
      commands.add(psl);                 // protocol switch limit
      commands.add("--np");
      commands.add(Integer.toString(np));   // no. of containers
      commands.add("--rank");
      commands.add(" " + Integer.toString(rank++)); // rank

      //temp sock port to share rank and ports
      commands.add("--pmiServer");
      commands.add(localhost);
      commands.add("--pmiServerPort");
      commands.add(String.valueOf(this.pmiServer.getPortNum()));

      if (appArgs != null) {
        commands.add("--appArgs");
        for (int i = 0; i < appArgs.length; i++) {
          commands.add(appArgs[i]);
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

    while (completedContainers < np) {
      // argument to allocate() is the progress indicator
      AllocateResponse response = rmClient.allocate(completedContainers / np);

      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        if (debugYarn) {
          System.out.println("\n[MPJAppMaster]: Container Id - " +
            status.getContainerId());
          System.out.println("[MPJAppMaster]: Container State - " +
            status.getState().toString());
          System.out.println("[MPJAppMaster]: Container Diagnostics - " +
            status.getDiagnostics());

        }
        ++completedContainers;
      }

      if (completedContainers != np) {
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
    MpichAppMaster am = new MpichAppMaster();
    am.init(args);
    am.run();
  }
}
