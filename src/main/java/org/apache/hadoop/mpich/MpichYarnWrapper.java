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

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class MpichYarnWrapper {

  private Socket clientSock;
  private int np;
  private String ioServer;
  private int ioServerPort;
  private String pmiServer;
  private String pmiServerPort;
  private String deviceName;
  private String className;
  private Class c;
  private String wdir;
  private int psl;
  private String rank;
  private String[] appArgs;
  private String portInfo;
  private Options opts;
  private CommandLine cliParser;

  public MpichYarnWrapper() {
    opts = new Options();

    opts.addOption("ioServer", true, "Hostname where the stdout and stderr " +
      "will be redirected");
    opts.addOption("ioServerPort", true, "Port required for a socket" +
      " redirecting IO");
    opts.addOption("psl", true, "Protocol Switch Limit");
    opts.addOption("np", true, "Number of Processes");
    opts.addOption("rank", true, "Rank of the process, it is set by AM");
    opts.addOption("pmiServer", true, "PMI Server hostname");
    opts.addOption("pmiServerPort", true, "Port required by NIODev to share" +
      "wireup information");
    opts.addOption("appArgs", true, "Specifies the User Application args");
    opts.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
  }

  public void init(String[] args) {
    try {
      cliParser = new GnuParser().parse(opts, args);

      np = Integer.parseInt(cliParser.getOptionValue("np"));
      ioServer = cliParser.getOptionValue("ioServer");
      ioServerPort = Integer.parseInt(cliParser.getOptionValue
        ("ioServerPort"));
      pmiServer = cliParser.getOptionValue("pmiServer");
      pmiServerPort = cliParser.getOptionValue("pmiServerPort");
      className = cliParser.getOptionValue("className");
      wdir = cliParser.getOptionValue("wdir");
      psl = Integer.parseInt(cliParser.getOptionValue("psl"));
      rank = cliParser.getOptionValue("rank");

      if (cliParser.hasOption("appArgs")) {
        appArgs = cliParser.getOptionValues("appArgs");
      }

      portInfo = "#Number of Processes;" + np +
        ";#Protocol Switch Limit;" + psl +
        ";#Server Name;" + ioServer +
        ";#Server Port;" + pmiServerPort;
    } catch (Exception exp) {
      exp.printStackTrace();
    }
  }

  public void run() {

    try {
      clientSock = new Socket(ioServer, ioServerPort);
    } catch (UnknownHostException exp) {
      System.err.println("Unknown Host Exception, Host not found");
      exp.printStackTrace();
    } catch (IOException exp) {
      exp.printStackTrace();
    }

    // Redirecting Output Stream
    try {
      System.setOut(new PrintStream(clientSock.getOutputStream(), true));
      System.setErr(new PrintStream(clientSock.getOutputStream(), true));
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      c = Class.forName(className);
    } catch (ClassNotFoundException exp) {
      exp.printStackTrace();
    }

    try {
      String[] arvs = new String[3];

      if (appArgs != null) {
        arvs = new String[3 + appArgs.length];
      }
      arvs[0] = rank;
      arvs[1] = portInfo;
      arvs[2] = deviceName;

      if (appArgs != null) {
        for (int i = 0; i < appArgs.length; i++) {
          arvs[3 + i] = appArgs[i];
        }
      }

      InetAddress localaddr = InetAddress.getLocalHost();
      String hostName = localaddr.getHostName();

      System.out.println("Starting process <" + rank + "> on <" + hostName + ">");

      Method m = c.getMethod("main", new Class[]{arvs.getClass()});
      m.setAccessible(true);
      int mods = m.getModifiers();

      if (m.getReturnType() != void.class || !Modifier.isStatic(mods)
        || !Modifier.isPublic(mods)) {
        throw new NoSuchMethodException("main");
      }

      m.invoke(null, new Object[]{arvs});

      System.out.println("Stopping process <" + rank + "> on <" + hostName + ">");

      System.out.println("EXIT");//Stopping IOThread

      try {
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }

  }

  public static void main(String args[]) throws Exception {
    MpichYarnWrapper wrapper = new MpichYarnWrapper();
    wrapper.init(args);
    wrapper.run();
  }
}
                                                                             
