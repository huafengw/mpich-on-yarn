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

import io.netty.channel.Channel;

public class MpiProcess {
  private ProcessApp app;
  private String host;   //The host that process will start on
  private int rank;
  private int pmiid;
  private Channel channel;
  private MpiProcessGroup group;

  public MpiProcess(int rank, int pmiid, String host, ProcessApp app, MpiProcessGroup group) {
    this.rank = rank;
    this.pmiid = pmiid;
    this.host = host;
    this.app = app;
    this.group = group;
  }

  public String getHost() {
    return host;
  }

  public int getRank() {
    return rank;
  }

  public int getPmiid() {
    return pmiid;
  }

  public MpiProcessGroup getGroup() {
    return group;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public ProcessApp getApp() {
    return app;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("MpiProcess info:\n");
    builder.append("     Rank: " + rank + "\n");
    builder.append("     Pmiid: " + rank + "\n");
    return builder.toString();
  }

  public boolean isSpawn() {
    return this.group.isSpawn();
  }
}
