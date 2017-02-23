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
  private Channel channel;
  private String host;   //The host that process will start on
  private int rank;
  private int pmiid;
  private MpiProcessGroup group;

  public MpiProcess(int rank, int pmiid, String host) {
    this.rank = rank;
    this.pmiid = pmiid;
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getRank() {
    return rank;
  }

  public void setRank(int rank) {
    this.rank = rank;
  }

  public int getPmiid() {
    return pmiid;
  }

  public void setPmiid(int pmiid) {
    this.pmiid = pmiid;
  }

  public MpiProcessGroup getGroup() {
    return group;
  }

  public void setGroup(MpiProcessGroup group) {
    this.group = group;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }
}
