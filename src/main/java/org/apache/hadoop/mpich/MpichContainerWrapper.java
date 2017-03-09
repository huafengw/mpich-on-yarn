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
package org.apache.hadoop.mpich;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MpichContainerWrapper {
  private static final Log LOG = LogFactory.getLog(MpichContainerWrapper.class);
  private Socket clientSock;
  private String np;
  private String ioServer;
  private int ioServerPort;
  private String pmiServer;
  private String pmiServerPort;
  private String executable;
  private String wdir;
  private String rank;
  private String pmiid;
  private boolean isSpawn = false;
  private String[] appArgs;
  private Options opts;
  private CommandLine cliParser;

  public MpichContainerWrapper() {
    opts = new Options();

    opts.addOption("ioServer", true, "Hostname where the stdout and stderr " +
      "will be redirected");
    opts.addOption("ioServerPort", true, "Port required for a socket" +
      " redirecting IO");
    opts.addOption("exec", true, "The actual executable to launch");
    opts.addOption("np", true, "Number of Processes");
    opts.addOption("rank", true, "Rank of the process, it is set by AM");
    opts.addOption("pmiid", true, "The unique id of the process");
    opts.addOption("pmiServer", true, "PMI Server hostname");
    opts.addOption("pmiServerPort", true, "Port required by NIODev to share" +
      "wireup information");
    opts.addOption("isSpawn", false, "Specify whether the process is the spawn one");
    opts.addOption("appArgs", true, "Specifies the User Application args");
    opts.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
  }

  public void init(String[] args) {
    try {
      cliParser = new GnuParser().parse(opts, args);

      np = cliParser.getOptionValue("np");
      ioServer = cliParser.getOptionValue("ioServer");
      ioServerPort = Integer.parseInt(cliParser.getOptionValue
        ("ioServerPort"));
      pmiServer = cliParser.getOptionValue("pmiServer");
      pmiServerPort = cliParser.getOptionValue("pmiServerPort");
      executable = cliParser.getOptionValue("exec");
      wdir = cliParser.getOptionValue("wdir");
      rank = cliParser.getOptionValue("rank");
      pmiid = cliParser.getOptionValue("pmiid");

      if (cliParser.hasOption("isSpawn")) {
        this.isSpawn = true;
      }

      if (cliParser.hasOption("appArgs")) {
        appArgs = cliParser.getOptionValues("appArgs");
      }
    } catch (Exception exp) {
      exp.printStackTrace();
    }
  }

  public void run() {
    try {
      clientSock = new Socket(ioServer, ioServerPort);
      System.setOut(new PrintStream(clientSock.getOutputStream(), true));
      System.setErr(new PrintStream(clientSock.getOutputStream(), true));

      String hostName = InetAddress.getLocalHost().getHostName();
      System.out.println("Starting process <" + rank + "> on <" + hostName + ">");

      List<String> commands = new ArrayList<String>();
      commands.add(executable);
      if (appArgs != null && appArgs.length > 0) {
        commands.addAll(Arrays.asList(appArgs));
      }

      ProcessBuilder processBuilder = new ProcessBuilder(commands);
      processBuilder.redirectErrorStream(true);
      processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

      Map<String, String> evns = processBuilder.environment();
      evns.put("PMI_RANK", rank);
      evns.put("PMI_SIZE", np);
      evns.put("PMI_ID", pmiid);
      evns.put("PMI_PORT", pmiServer + ":" + pmiServerPort);

      if (this.isSpawn) {
        evns.put("PMI_SPAWNED", "1");
      }

      LOG.info("Starting process:");
      for (String cmd: commands) {
        LOG.info(cmd + "\n");
      }

      Process process = processBuilder.start();
      System.out.println(process.waitFor());
      System.out.println("EXIT");//Stopping IOThread
      clientSock.close();
    } catch (UnknownHostException exp) {
      System.err.println("Unknown Host Exception, Host not found");
      exp.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String args[]) throws Exception {
    MpichContainerWrapper wrapper = new MpichContainerWrapper();
    wrapper.init(args);
    wrapper.run();
  }
}
                                                                             
