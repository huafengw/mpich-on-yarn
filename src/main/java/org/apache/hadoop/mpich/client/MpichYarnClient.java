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
package org.apache.hadoop.mpich.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MpichYarnClient {
  //conf fetches information from yarn-site.xml and yarn-default.xml.
  private Configuration conf;

  //Number of containers
  private int np;
  private String localHost;
  private String className;
  private String workingDirectory;
  private int psl;
  private String[] appArgs;
  private int amMem;
  private int amCores;
  private String containerMem;
  private String containerCores;
  private String yarnQueue;
  private String appName;
  private int amPriority;
  private String mpjContainerPriority;
  private String hdfsFolder;
  private boolean debugYarn = false;
  private Log logger = null;

  public static boolean isRunning = false;
  private FinalApplicationStatus fStatus;
  private Options opts = null;
  private CommandLine cliParser = null;
  private IOMessageHandler ioMessageHandler = null;

  public MpichYarnClient() {
    logger = LogFactory.getLog(MpichYarnClient.class);
    conf = new YarnConfiguration();

    opts = new Options();

    opts.addOption("np", true, "Number of Processes");
    opts.addOption("className", true, "Main Class name");
    opts.addOption("wdir", true, "Specifies the current working directory");
    opts.addOption("psl", true, "Specifies the Protocol Switch Limit");
    opts.addOption("jarPath", true, "Specifies the Path to user's Jar File");
    opts.addOption("appArgs", true, "Specifies the User Application args");
    opts.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
    opts.addOption("amMem", true, "Specifies AM container memory");
    opts.addOption("amCores", true, "Specifies AM container virtual cores");
    opts.addOption("containerMem", true, "Specifies containers memory");
    opts.addOption("containerCores", true, "Specifies containers v-cores");
    opts.addOption("yarnQueue", true, "Specifies the yarn queue");
    opts.addOption("appName", true, "Specifies the application name");
    opts.addOption("amPriority", true, "Specifies AM container priority");
    opts.addOption("containerPriority", true, "Specifies the prioirty of" +
      "containers running MPI processes");
    opts.addOption("hdfsFolder", true, "Specifies the HDFS folder where AM," +
      "Wrapper and user code jar files will be uploaded");
    opts.addOption("debugYarn", false, "Specifies the debug flag");
  }

  public void init(String[] args) {
    try {
      cliParser = new GnuParser().parse(opts, args);

      np = Integer.parseInt(cliParser.getOptionValue("np"));

      localHost = InetAddress.getLocalHost().getHostName();

      className = cliParser.getOptionValue("className");

      workingDirectory = cliParser.getOptionValue("wdir");

      psl = Integer.parseInt(cliParser.getOptionValue("psl"));

      amMem = Integer.parseInt(cliParser.getOptionValue("amMem", "2048"));

      amCores = Integer.parseInt(cliParser.getOptionValue("amCores", "1"));

      containerMem = cliParser.getOptionValue("containerMem", "1024");

      containerCores = cliParser.getOptionValue("containerCores", "1");

      yarnQueue = cliParser.getOptionValue("yarnQueue", "default");

      appName = cliParser.getOptionValue("appName", "MPJ-YARN-Application");

      amPriority = Integer.parseInt(cliParser.getOptionValue
        ("amPriority", "0"));

      mpjContainerPriority = cliParser.getOptionValue
        ("mpjContainerPriority", "0");

      hdfsFolder = cliParser.getOptionValue("hdfsFolder", "/");

      if (cliParser.hasOption("appArgs")) {
        appArgs = cliParser.getOptionValues("appArgs");
      }

      if (cliParser.hasOption("debugYarn")) {
        debugYarn = true;
      }

      ioMessageHandler = new IOMessageHandler(np);
    } catch (Exception exp) {
      exp.printStackTrace();
    }
  }

  public void run() throws Exception {
    FileSystem fs = FileSystem.get(conf);

    Path source = new Path("/lib/mpj-app-master.jar");
    String pathSuffix = hdfsFolder + "mpj-app-master.jar";
    Path dest = new Path(fs.getHomeDirectory(), pathSuffix);

    if (debugYarn) {
      logger.info("Uploading mpj-app-master.jar to: " + dest.toString());
    }

    fs.copyFromLocalFile(false, true, source, dest);
    FileStatus destStatus = fs.getFileStatus(dest);

    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    if (debugYarn) {
      YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();
      logger.info("\nNodes Information");
      logger.info("Number of NM: " + metrics.getNumNodeManagers() + "\n");

      List<NodeReport> nodeReports = yarnClient.getNodeReports
        (NodeState.RUNNING);
      for (NodeReport n : nodeReports) {
        logger.info("NodeId: " + n.getNodeId());
        logger.info("RackName: " + n.getRackName());
        logger.info("Total Memory: " + n.getCapability().getMemory());
        logger.info("Used Memory: " + n.getUsed().getMemory());
        logger.info("Total vCores: " + n.getCapability().getVirtualCores());
        logger.info("Used vCores: " + n.getUsed().getVirtualCores() + "\n");
      }
    }

    // Create application via yarnClient
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    int maxMem = appResponse.getMaximumResourceCapability().getMemory();

    if (debugYarn) {
      logger.info("Max memory capability resources in cluster: " + maxMem);
    }

    if (amMem > maxMem) {
      amMem = maxMem;
      logger.info("AM memory specified above threshold of cluster " +
        "Using maximum memory for AM container: " + amMem);
    }
    int maxVcores = appResponse.getMaximumResourceCapability().getVirtualCores();

    if (debugYarn) {
      logger.info("Max vCores capability resources in cluster: " + maxVcores);
    }

    if (amCores > maxVcores) {
      amCores = maxVcores;
      logger.info("AM virtual cores specified above threshold of cluster " +
        "Using maximum virtual cores for AM container: " + amCores);
    }

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    List<String> commands = new ArrayList<String>();
    commands.add("$JAVA_HOME/bin/java");
    commands.add("-Xmx" + amMem + "m");
    commands.add("runtime.starter.MPJAppMaster");
    commands.add("--np");
    commands.add(String.valueOf(np));
    commands.add("--ioServer");
    commands.add(localHost); //server name
    commands.add("--ioServerPort");
    commands.add(Integer.toString(ioMessageHandler.getPortNum())); //server port
    commands.add("--className");
    commands.add(className); //class name
    commands.add("--wdir");
    commands.add(workingDirectory); //wdir
    commands.add("--psl");
    commands.add(Integer.toString(psl)); //protocol switch limit
    commands.add("--mpjContainerPriority");
    commands.add(mpjContainerPriority);// priority for mpj containers
    commands.add("--containerMem");
    commands.add(containerMem);
    commands.add("--containerCores");
    commands.add(containerCores);

    if (debugYarn) {
      commands.add("--debugYarn");
    }

    if (appArgs != null) {
      commands.add("--appArgs");
      Collections.addAll(commands, appArgs);
    }

    amContainer.setCommands(commands); //set commands

    // Setup local Resource for ApplicationMaster
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);

    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(dest));
    appMasterJar.setSize(destStatus.getLen());
    appMasterJar.setTimestamp(destStatus.getModificationTime());
    appMasterJar.setType(LocalResourceType.ARCHIVE);
    appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);

    amContainer.setLocalResources(
      Collections.singletonMap("mpj-app-master.jar", appMasterJar));

    // Setup CLASSPATH for ApplicationMaster
    // Setting up the environment
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMem);
    capability.setVirtualCores(amCores);

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext =
      app.getApplicationSubmissionContext();

    appContext.setApplicationName(appName);
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue(yarnQueue); // queue

    Priority priority = Priority.newInstance(amPriority);
    appContext.setPriority(priority);

    ApplicationId appId = appContext.getApplicationId();

    //Adding ShutDown Hook
    Runtime.getRuntime().addShutdownHook(new KillYarnApp(appId, yarnClient));

    // Submit application
    System.out.println("Submitting Application: " +
      appContext.getApplicationName() + "\n");

    try {
      isRunning = true;
      yarnClient.submitApplication(appContext);
    } catch (Exception exp) {
      System.err.println("Error Submitting Application");
      exp.printStackTrace();
    }

    this.ioMessageHandler.acceptAll();
    // wait for all IO Threads to complete
    this.ioMessageHandler.join();
    isRunning = true;

    System.out.println("Application Statistics!\n");
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      YarnApplicationState appState = appReport.getYarnApplicationState();
      fStatus = appReport.getFinalApplicationStatus();
      if (appState == YarnApplicationState.FINISHED) {
        isRunning = false;
        if (fStatus == FinalApplicationStatus.SUCCEEDED) {
          System.out.println("State: " + fStatus);
        } else {
          System.out.println("State: " + fStatus);
        }
        break;
      } else if (appState == YarnApplicationState.KILLED) {
        isRunning = false;
        System.out.println("State: " + appState);
        break;
      } else if (appState == YarnApplicationState.FAILED) {
        isRunning = false;
        System.out.println("State: " + appState);
        break;
      }
      Thread.sleep(100);
    }

    try {
      if (debugYarn) {
        logger.info("Cleaning the files from hdfs: ");
        logger.info("1) " + dest.toString());
      }

      fs.delete(dest);
    } catch (IOException exp) {
      exp.printStackTrace();
    }
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    for (String c : conf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim());
    }

    Apps.addToEnvironment(appMasterEnv,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*");
  }

  public static void main(String[] args) throws Exception {
    MpichYarnClient client = new MpichYarnClient();
    client.init(args);
    client.run();
  }
}

