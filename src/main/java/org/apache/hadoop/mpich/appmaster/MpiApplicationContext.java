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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.util.Map;

public class MpiApplicationContext {
  private Configuration conf;
  private Resource containerResource;
  private Priority priority;
  private Map<String, LocalResource> localResources;

  private String ioServer;
  private int ioServerPort;
  private String pmiServer;
  private int pmiServerPort;

  public MpiApplicationContext(String ioServer, int ioServerPort,
      String pmiServer, int pmiServerPort,
      Configuration conf, Resource containerResource,
      Priority priority, Map<String, LocalResource> localResources) {
    this.ioServer = ioServer;
    this.ioServerPort = ioServerPort;
    this.pmiServer = pmiServer;
    this.pmiServerPort = pmiServerPort;
    this.conf = conf;
    this.containerResource = containerResource;
    this.priority = priority;
    this.localResources = localResources;
  }

  public Configuration getConf() {
    return conf;
  }

  public Resource getContainerResource() {
    return containerResource;
  }

  public Priority getPriority() {
    return priority;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public String getIoServer() {
    return ioServer;
  }

  public int getIoServerPort() {
    return ioServerPort;
  }

  public String getPmiServer() {
    return pmiServer;
  }

  public int getPmiServerPort() {
    return pmiServerPort;
  }
}
