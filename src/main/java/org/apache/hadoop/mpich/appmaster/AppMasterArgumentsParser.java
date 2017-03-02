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

import org.apache.commons.cli.*;

public class AppMasterArgumentsParser {
  private static Options OPTS = new Options();

  static {
    OPTS.addOption("np", true, "Number of Processes");
    OPTS.addOption("ioServer", true, "Hostname required for Server Socket");
    OPTS.addOption("ioServerPort", true, "Port required for a socket" +
      " redirecting IO");
    OPTS.addOption("wdir", true, "Specifies the current working directory");
    OPTS.addOption("psl", true, "Specifies the Protocol Switch Limit");
    OPTS.addOption("appArgs", true, "Specifies the User Application args");
    OPTS.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
    OPTS.addOption("containerMem", true, "Specifies mpj containers memory");
    OPTS.addOption("containerCores", true, "Specifies mpj containers v-cores");
    OPTS.addOption("mpjContainerPriority", true, "Specifies the prioirty of" +
      "containers running MPI processes");
    OPTS.addOption("debugYarn", false, "Specifies the debug flag");
  }

  public static AppMasterArguments parse(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(OPTS, args);
    int np = Integer.parseInt(cliParser.getOptionValue("np"));
    String wdir = cliParser.getOptionValue("wdir");
    String psl = cliParser.getOptionValue("psl");
    int containerMem = Integer.parseInt(cliParser.getOptionValue
      ("containerMem", "1024"));
    int containerCores = Integer.parseInt(cliParser.getOptionValue
      ("containerCores", "1"));
    int mpjContainerPriority = Integer.parseInt(cliParser.getOptionValue
      ("mpjContainerPriority", "0"));
    String ioServer = cliParser.getOptionValue("ioServer");
    int ioServerPort = Integer.parseInt(cliParser.getOptionValue("ioServerPort"));

    String[] appArgs = null;
    if (cliParser.hasOption("appArgs")) {
      appArgs = cliParser.getOptionValues("appArgs");
    }

    boolean debugYarn = false;
    if (cliParser.hasOption("debugYarn")) {
      debugYarn = true;
    }

    return new AppMasterArguments(np, wdir, psl, containerMem, containerCores,
      mpjContainerPriority, ioServer, ioServerPort, appArgs, debugYarn);
  }
}
