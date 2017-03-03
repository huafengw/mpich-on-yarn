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

public class AppMasterArguments {
  private int np;
  private String wdir;
  private int containerMem;
  private int containerCores;
  private int mpjContainerPriority;
  private String ioServer;
  private int ioServerPort;
  private String[] appArgs;
  private boolean debugYarn;
  private String executable;

  public AppMasterArguments(int np, String wdir, int containerMem,
      int containerCores, int mpjContainerPriority, String ioServer,
      int ioServerPort, String[] appArgs, boolean debugYarn) {
    this.np = np;
    this.wdir = wdir;
    this.containerMem = containerMem;
    this.containerCores = containerCores;
    this.mpjContainerPriority = mpjContainerPriority;
    this.ioServer = ioServer;
    this.ioServerPort = ioServerPort;
    this.appArgs = appArgs;
    this.debugYarn = debugYarn;
  }

  public int getNp() {
    return np;
  }

  public String getWdir() {
    return wdir;
  }

  public int getContainerMem() {
    return containerMem;
  }

  public int getContainerCores() {
    return containerCores;
  }

  public int getMpjContainerPriority() {
    return mpjContainerPriority;
  }

  public String getIoServer() {
    return ioServer;
  }

  public int getIoServerPort() {
    return ioServerPort;
  }

  public String[] getAppArgs() {
    return appArgs;
  }

  public boolean isDebugYarn() {
    return debugYarn;
  }

  public String getExecutable() {
    return executable;
  }
}
