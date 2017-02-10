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
package org.apache.hadoop.mpich.appmaster.pmi;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hadoop.mpich.appmaster.MpiProcess;
import org.apache.hadoop.mpich.appmaster.MpiProcessManager;
import org.apache.hadoop.mpich.util.Utils;

import java.util.Map;

public class PMIServerHandler extends ChannelInboundHandlerAdapter {
  private MpiProcessManager processManager;

  public PMIServerHandler(MpiProcessManager manager) {
    this.processManager = manager;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.out.println((String) msg);
    Map<String, String> kvs = Utils.parseKeyVals((String) msg);
    ClientToServerCommand command = Utils.getCommand(kvs);
    switch (command) {
      case INITACK:
        Integer pmiid = Integer.parseInt(kvs.get("pmiid"));
        MpiProcess process = this.processManager.getProcess(pmiid);
        this.processManager.addClient(pmiid, ctx.channel());
        ctx.write("cmd=initack\n");
        ctx.write("cmd=set size=" + this.processManager.getNumProcesses() + "\n");
        ctx.write("cmd=set rank=" + process.getRank() + "\n");
        ctx.write("cmd=set debug=1\n");
        break;
      case INIT:
        String pmi_version = kvs.get("pmi_version");
        String pmi_subversion = kvs.get("pmi_subversion");
        ctx.write("cmd=response_to_init pmi_version=" + pmi_version +" pmi_subversion=" +
          pmi_subversion + " rc=0\n");
        break;
      case GET_MAXES:
        ctx.write("cmd=maxes kvsname_max=256 keylen_max=64 vallen_max=256\n");
        break;
      case GET_APPNUM:
        ctx.write("cmd=appnum appnum=0\n");
        break;
      case GET_MY_KVS_NAME:

        break;
      case PUT:
        break;
      case GET:
        break;
      case BARRIER_IN:
        break;
        
      case FINALIZE:
        break;
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }

}
