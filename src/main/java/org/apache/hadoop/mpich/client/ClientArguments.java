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

public class ClientArguments {
  private int np;
  private String executable;
  private String workingDirectory;
  private String[] appArgs;
  private int amMem;
  private int amCores;
  private int containerMem;
  private int containerCores;
  private String yarnQueue;
  private String appName;
  private int amPriority;
  private String containerPriority;
  private String hdfsFolder;
  private boolean debugYarn;

  public ClientArguments(int np, String executable, String workingDirectory, String[] appArgs,
      int amMem, int amCores, int containerMem, int containerCores, String yarnQueue,
      String appName, int amPriority, String containerPriority, String hdfsFolder,
      boolean debugYarn) {
    this.np = np;
    this.executable = executable;
    this.workingDirectory = workingDirectory;
    this.appArgs = appArgs;
    this.amMem = amMem;
    this.amCores = amCores;
    this.containerMem = containerMem;
    this.containerCores = containerCores;
    this.yarnQueue = yarnQueue;
    this.appName = appName;
    this.amPriority = amPriority;
    this.containerPriority = containerPriority;
    this.hdfsFolder = hdfsFolder;
    this.debugYarn = debugYarn;
  }

  public int getNp() {
    return np;
  }

  public String getExecutable() {
    return executable;
  }

  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public String[] getAppArgs() {
    return appArgs;
  }

  public int getAmMem() {
    return amMem;
  }

  public int getAmCores() {
    return amCores;
  }

  public int getContainerMem() {
    return containerMem;
  }

  public int getContainerCores() {
    return containerCores;
  }

  public String getYarnQueue() {
    return yarnQueue;
  }

  public String getAppName() {
    return appName;
  }

  public int getAmPriority() {
    return amPriority;
  }

  public String getContainerPriority() {
    return containerPriority;
  }

  public String getHdfsFolder() {
    return hdfsFolder;
  }

  public boolean isDebugYarn() {
    return debugYarn;
  }
}
