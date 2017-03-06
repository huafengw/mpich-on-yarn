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

import org.apache.commons.cli.*;

public class ClientArgumentsParser {
  private static Options OPTS = new Options();
  
  static {
    OPTS.addOption("np", true, "Number of Processes");
    OPTS.addOption("exec", true, "The mpich executable file to run");
    OPTS.addOption("wdir", true, "Specifies the current working directory");
    OPTS.addOption("appArgs", true, "Specifies the User Application args");
    OPTS.getOption("appArgs").setArgs(Option.UNLIMITED_VALUES);
    OPTS.addOption("amMem", true, "Specifies AM container memory");
    OPTS.addOption("amCores", true, "Specifies AM container virtual cores");
    OPTS.addOption("containerMem", true, "Specifies containers memory");
    OPTS.addOption("containerCores", true, "Specifies containers v-cores");
    OPTS.addOption("yarnQueue", true, "Specifies the yarn queue");
    OPTS.addOption("appName", true, "Specifies the application name");
    OPTS.addOption("amPriority", true, "Specifies AM container priority");
    OPTS.addOption("containerPriority", true, "Specifies the prioirty of" +
      "containers running MPI processes");
    OPTS.addOption("hdfsFolder", true, "Specifies the HDFS folder where AM," +
      "Wrapper and user code jar files will be uploaded");
    OPTS.addOption("debugYarn", false, "Specifies the debug flag");
  }

  public static ClientArguments parse(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(OPTS, args);

    int np = Integer.parseInt(cliParser.getOptionValue("np"));

    String executable = cliParser.getOptionValue("exec");

    String workingDirectory = cliParser.getOptionValue("wdir");

    int amMem = Integer.parseInt(cliParser.getOptionValue("amMem", "2048"));

    int amCores = Integer.parseInt(cliParser.getOptionValue("amCores", "1"));

    int containerMem = Integer.parseInt(cliParser.getOptionValue("containerMem", "1024"));

    int containerCores = Integer.parseInt(cliParser.getOptionValue("containerCores", "1"));

    String yarnQueue = cliParser.getOptionValue("yarnQueue", "default");

    String appName = cliParser.getOptionValue("appName", "MPICH-YARN-Application");

    int amPriority = Integer.parseInt(cliParser.getOptionValue
      ("amPriority", "0"));

    String containerPriority = cliParser.getOptionValue
      ("mpjContainerPriority", "0");

    String hdfsFolder = cliParser.getOptionValue("hdfsFolder", "/tmp");

    String[] appArgs = null;
    if (cliParser.hasOption("appArgs")) {
      appArgs = cliParser.getOptionValues("appArgs");
    }

    boolean debugYarn = false;
    if (cliParser.hasOption("debugYarn")) {
      debugYarn = true;
    }

    return new ClientArguments(np, executable, workingDirectory, appArgs, amMem, amCores,
      containerMem, containerCores, yarnQueue, appName, amPriority, containerPriority,
      hdfsFolder, debugYarn);
  }
}
