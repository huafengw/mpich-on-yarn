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
package org.apache.hadoop.mpich.appmaster.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hadoop.mpich.MpiProcess;
import org.apache.hadoop.mpich.MpiProcessGroup;
import org.apache.hadoop.mpich.appmaster.MpiProcessManager;
import org.apache.hadoop.mpich.appmaster.MpiProcessWorldLauncher;
import org.apache.hadoop.mpich.util.KVStoreFactory;

import java.util.ArrayList;
import java.util.List;

public class PMIServer {
  private int portNum;
  private MpiProcessManager manager;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel channel;

  public PMIServer(MpiProcessManager manager, int portNum) {
    this.manager = manager;
    this.portNum = portNum;
  }

  public int getPortNum() {
    return this.portNum;
  }

  public void start() throws Exception {
    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ServerChannelInitializer(this.manager))
      .childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    this.channel = b.bind(portNum).sync().channel();
  }

  public void stop() throws InterruptedException {
    channel.close().sync();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
  }

  private void waitUntilClose() throws InterruptedException {
    this.channel.closeFuture().sync();
  }

  public static void main(String[] args) throws Exception {
    List<MpiProcess> processes = new ArrayList<MpiProcess>();
    processes.add(new MpiProcess(0, 0, "host1"));
    processes.add(new MpiProcess(1, 1, "host2"));
    MpiProcessGroup group = new MpiProcessGroup(processes, KVStoreFactory.newKVStore());
    MpiProcessManager manager = new MpiProcessManager();
    manager.addMpiProcessGroup(group);
    PMIServer server = new PMIServer(manager, 0);
    server.start();
    System.out.println(server.getPortNum());
    server.waitUntilClose();
  }
}
