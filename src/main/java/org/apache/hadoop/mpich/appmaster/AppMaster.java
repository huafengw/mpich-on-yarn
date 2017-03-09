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
import org.apache.hadoop.mpich.ProcessApp;
import org.apache.hadoop.mpich.ProcessWorld;
import org.apache.hadoop.mpich.appmaster.netty.PMIServer;
import org.apache.hadoop.mpich.util.Utils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;

public class AppMaster {
  private Configuration conf;
  private String pmServerHost;
  private int pmServerPort;
  private Socket ioServerSock;
  private PMIServer pmiServer;
  private AppMasterArguments appArguments;
  private AMRMClientWrapper amrmClientWrapper;
  private MpiProcessManager mpiProcessManager;

  public AppMaster(AppMasterArguments arguments) {
    this.conf = new YarnConfiguration();
    this.appArguments = arguments;
    this.amrmClientWrapper = new AMRMClientWrapper(this.conf, this.appArguments);
  }

  public void run() throws Exception {
    try {
      //redirecting stdout and stderr to client
      ioServerSock = new Socket(appArguments.getIoServer(), appArguments.getIoServerPort());
      System.setOut(new PrintStream(ioServerSock.getOutputStream(), true));
      System.setErr(new PrintStream(ioServerSock.getOutputStream(), true));

      pmServerHost = InetAddress.getLocalHost().getHostName();
      pmServerPort = Utils.findFreePort();

      ContainerAllocator allocator = this.amrmClientWrapper.register(pmServerHost, pmServerPort);
      this.mpiProcessManager = new MpiProcessManager(allocator);
      this.pmiServer = new PMIServer(mpiProcessManager, pmServerPort);
      this.pmiServer.start();

      this.mpiProcessManager.launch(getProcessWorldAccordingArgs(appArguments));
    } catch (Exception exp) {
      exp.printStackTrace();
    }

    while (!this.mpiProcessManager.allAppFinished()) {
      Thread.sleep(200);
    }

    // Un-register with ResourceManager
    this.amrmClientWrapper.unregister();
    this.mpiProcessManager.close();
    this.pmiServer.stop();
    System.out.println("Application finished");
    System.out.println("EXIT");
    this.ioServerSock.close();
  }

  private ProcessWorld getProcessWorldAccordingArgs(AppMasterArguments arguments) {
    ProcessWorld processWorld = new ProcessWorld();
    ProcessApp processApp = new ProcessApp();
    processApp.setExeName(arguments.getExecutable());
    processApp.setNumProcess(arguments.getNp());
    if (arguments.getAppArgs() != null) {
      processApp.setArgNum(arguments.getAppArgs().length);
      processApp.setArgs(Arrays.asList(arguments.getAppArgs()));
    } else {
      processApp.setArgNum(0);
    }
    processWorld.addProcessApp(processApp);

    return processWorld;
  }

  public static void main(String[] args) throws Exception {
    AppMasterArguments arguments = AppMasterArgumentsParser.parse(args);
    AppMaster am = new AppMaster(arguments);
    am.run();
  }
}
