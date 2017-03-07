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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mpich.appmaster.AppMaster;
import org.apache.hadoop.mpich.util.Constants;
import org.apache.hadoop.mpich.util.Utils;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class Client {
  private Log logger;
  //conf fetches information from yarn-site.xml and yarn-default.xml.
  private Configuration conf;
  private ClientArguments arguments;
  private String localHost;
  private String appMasterJarPath;

  public static boolean isRunning = false;
  private IOMessageHandler ioMessageHandler = null;

  public Client(ClientArguments arguments) {
    this.logger = LogFactory.getLog(Client.class);
    this.conf = new YarnConfiguration();
    this.arguments = arguments;
    this.appMasterJarPath = ClassUtil.findContainingJar(AppMaster.class);
  }

  public void run() throws Exception {
    localHost = InetAddress.getLocalHost().getHostName();
    ioMessageHandler = new IOMessageHandler(arguments.getNp());

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    this.verifyClusterResources(appResponse);
    ContainerLaunchContext context = createContainerLaunchContext(appResponse);
    ApplicationSubmissionContext appContext = createAppSubmissionContext(app, context);

    ApplicationId appId = appContext.getApplicationId();

    //Adding ShutDown Hook
    Runtime.getRuntime().addShutdownHook(new KillYarnApp(appId, yarnClient));

    // Submit application
    System.out.println("Submitting Application: " + appContext.getApplicationName() + "\n");

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
    FinalApplicationStatus fStatus = null;
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
  }

  private ApplicationSubmissionContext createAppSubmissionContext(
      YarnClientApplication app, ContainerLaunchContext context) {
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(arguments.getAmMem());
    capability.setVirtualCores(arguments.getAmCores());

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

    appContext.setApplicationName(arguments.getAppName());
    appContext.setAMContainerSpec(context);
    appContext.setResource(capability);
    appContext.setQueue(arguments.getYarnQueue());

    Priority priority = Priority.newInstance(arguments.getAmPriority());
    appContext.setPriority(priority);
    return appContext;
  }

  private ContainerLaunchContext createContainerLaunchContext(GetNewApplicationResponse response) throws IOException {
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    List<String> commands = new ArrayList<String>();
    commands.add("$JAVA_HOME/bin/java");
    commands.add("-Xmx" + arguments.getAmMem() + "m");
    commands.add(AppMaster.class.getName());
    commands.add("--np");
    commands.add(String.valueOf(arguments.getNp()));
    commands.add("--ioServer");
    commands.add(localHost); //server name
    commands.add("--ioServerPort");
    commands.add(Integer.toString(ioMessageHandler.getPortNum())); //server port
    commands.add("--exec");
    commands.add(arguments.getExecutable()); //class name
    commands.add("--wdir");
    commands.add(arguments.getWorkingDirectory()); //wdir
    commands.add("--containerPriority");
    commands.add(arguments.getContainerPriority());// priority for mpj containers
    commands.add("--containerMem");
    commands.add(Integer.toString(arguments.getContainerMem()));
    commands.add("--containerCores");
    commands.add(Integer.toString(arguments.getContainerCores()));

    if (arguments.isDebugYarn()) {
      commands.add("--debugYarn");
    }

    if (arguments.getAppArgs() != null) {
      commands.add("--appArgs");
      Collections.addAll(commands, arguments.getAppArgs());
    }

    commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    amContainer.setCommands(commands); //set commands

    FileSystem fs = FileSystem.get(conf);
    Path source = new Path(this.appMasterJarPath);
    String pathSuffix = arguments.getHdfsFolder() + "/AppMaster.jar";
    Path dest = new Path(fs.getHomeDirectory(), pathSuffix);

    logger.info("Uploading " + appMasterJarPath + " to: " + dest.toString());
    fs.copyFromLocalFile(false, true, source, dest);

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Utils.addToLocalResources(fs, "AppMaster.jar", dest, localResources);
    amContainer.setLocalResources(localResources);

    // Setup CLASSPATH for ApplicationMaster
    // Setting up the environment
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    appMasterEnv.put(Constants.APP_JAR_LOCATION, dest.toUri().toString());
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    logger.info("==========================================================================");
    logger.info("YARN AM launch context:");
    logger.info("    env:");
    for (String key: appMasterEnv.keySet()) {
      logger.info("     " + key + " -> " + appMasterEnv.get(key));
    }
    logger.info("    resources:");
    for (String key: localResources.keySet()) {
      logger.info("     " + key + " -> " + localResources.get(key));
    }
    logger.info("==========================================================================");

    return amContainer;
  }

  private void verifyClusterResources(GetNewApplicationResponse response) {
    int maxMem = response.getMaximumResourceCapability().getMemory();
    if (arguments.getAmMem() > maxMem) {
      throw new IllegalArgumentException("Required AM memory " + arguments.getAmMem() +
        " is above the max threshold " + maxMem + " of this cluster! " +
        "Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or " +
        "'yarn.nodemanager.resource.memory-mb'.");
    }

    int maxVcores = response.getMaximumResourceCapability().getVirtualCores();
    if (arguments.getAmCores() > maxVcores) {
      throw new IllegalArgumentException("Required AM cores " + arguments.getAmCores() +
        " is above the max threshold " + maxVcores + "of this cluster! ");
    }
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
      .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }

    appMasterEnv.put("CLASSPATH", classPathEnv.toString());
  }

  public static void main(String[] args) throws Exception {
    ClientArguments arguments = ClientArgumentsParser.parse(args);
    Client client = new Client(arguments);
    client.run();
  }
}

