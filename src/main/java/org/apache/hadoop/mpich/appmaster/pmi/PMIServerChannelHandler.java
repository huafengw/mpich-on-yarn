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
import org.apache.hadoop.mpich.appmaster.MpiProcessGroup;
import org.apache.hadoop.mpich.appmaster.MpiProcessManager;
import org.apache.hadoop.mpich.util.KVPair;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.PMIResponseBuilder;
import org.apache.hadoop.mpich.util.Utils;

import java.util.Map;

public class PMIServerChannelHandler extends ChannelInboundHandlerAdapter {
  private MpiProcessManager manager;

  public PMIServerChannelHandler(MpiProcessManager manager) {
    this.manager = manager;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.out.println((String)msg + " from " + ctx.channel().id().toString());
    Map<String, String> kvs = Utils.parseKeyVals((String) msg);
    ClientToServerCommand command = Utils.getCommand(kvs);
    switch (command) {
      case INITACK:
        this.onInitAck(kvs, ctx);
        break;
      case INIT:
        this.onInit(kvs, ctx);
        break;
      case GET_MAXES:
        this.onGetMaxes(kvs, ctx);
        break;
      case GET_APPNUM:
       this.onGetAppNum(kvs, ctx);
        break;
      case CREATE_KVS:
        this.onCreateKVS(kvs, ctx);
        break;
      case DESTORY_KVS:
        this.onDestoryKVS(kvs, ctx);
        break;
      case GET_MY_KVSNAME:
        this.onGetMyKvsName(kvs, ctx);
        break;
      case GETBYIDX:
        this.onGetByIdx(kvs, ctx);
        break;
      case PUT:
        this.onPut(kvs, ctx);
        break;
      case GET:
        this.onGet(kvs, ctx);
        break;
      case BARRIER_IN:
        this.onBarrierIn(kvs, ctx);
        break;
      case SPAWN:
        // Todo
        break;
      case ABORT:
        this.onAbort(kvs, ctx);
        break;
      case GET_UNIVERSE_SIZE:
        this.onGetUniverseSize(kvs, ctx);
        break;
      case FINALIZE:
        this.onFinalize(kvs, ctx);
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

  private void onInitAck(Map<String, String> kvs, ChannelHandlerContext ctx) {
    Integer pmiid = Integer.parseInt(kvs.get("pmiid"));
    MpiProcess process = this.manager.getProcessById(pmiid);
    manager.addClient(pmiid, ctx.channel());
    ctx.write(new PMIResponseBuilder().append("cmd", "initack").build());
    ctx.write(new PMIResponseBuilder().append("cmd", "set")
      .append("size", String.valueOf(process.getGroup().getNumProcesses())).build());
    ctx.write(new PMIResponseBuilder().append("cmd", "set")
      .append("rank", String.valueOf(process.getRank())).build());
    ctx.write(new PMIResponseBuilder().append("cmd", "set")
      .append("debug", "1").build());
  }

  private void onInit(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String pmi_version = kvs.get("pmi_version");
    String pmi_subversion = kvs.get("pmi_subversion");
    String response = new PMIResponseBuilder()
      .append("cmd", "response_to_init")
      .append("pmi_version", pmi_version)
      .append("pmi_subversion", pmi_subversion)
      .append("rc", "0").build();
    ctx.write(response);
  }

  private void onGetMaxes(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String response = new PMIResponseBuilder()
      .append("cmd", "maxes")
      .append("kvsname_max", "256")
      .append("keylen_max", "64")
      .append("vallen_max", "256").build();
    ctx.write(response);
  }

  private void onGetAppNum(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String response = new PMIResponseBuilder().
      append("cmd", "appnum").append("appnum", "0").build();
    ctx.write(response);
  }

  private void onGetMyKvsName(Map<String, String> kvs, ChannelHandlerContext ctx) {
    MpiProcess process = this.manager.getProcessByChannel(ctx.channel());
    if (process != null) {
      String storeName = process.getGroup().getKvStore().getName();
      String response = new PMIResponseBuilder().
        append("cmd", "my_kvsname").append("kvsname", storeName).build();
      ctx.write(response);
    }
  }

  private void onGetByIdx(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String kvsName = kvs.get("kvsname");
    KVStore kvStore = this.manager.getKvStore(kvsName);
    PMIResponseBuilder responseBuilder = new PMIResponseBuilder()
      .append("cmd", "getbyidx_results");
    if (kvStore != null) {
      Integer idx = Integer.getInteger(kvs.get("idx"));
      KVPair result = kvStore.getKVPairByIdx(idx);
      if (result != null) {
        responseBuilder.append("rc", "0")
          .append("nextidx", Integer.toString(idx + 1))
          .append("key", result.getKey())
          .append("val", result.getValue());
      } else {
        responseBuilder.append("rc", "-1")
          .append("reason", "no_more_keyvals");
      }
    } else {
      responseBuilder.append("rc", "-1")
        .append("reason", "kvs_" + kvsName + "_not_found");
    }
    ctx.write(responseBuilder.build());
  }

  private void onBarrierIn(Map<String, String> kvs, ChannelHandlerContext ctx) {
    MpiProcess process = this.manager.getProcessByChannel(ctx.channel());
    if (process != null) {
      MpiProcessGroup group = process.getGroup();
      Integer numInBarrier = group.getnInBarrier().incrementAndGet();
      if (numInBarrier == group.getNumProcesses()) {
        String response = new PMIResponseBuilder().append("cmd", "barrier_out").build();
        for (MpiProcess proc : group.getProcesses()) {
          proc.getChannel().write(response);
          proc.getChannel().flush();
        }
        group.getnInBarrier().set(0);
      }
    }
  }

  private void onFinalize(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String response = new PMIResponseBuilder()
      .append("cmd", "finalize_ack").build();
    ctx.write(response);
  }

  private void onPut(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String kvsName = kvs.get("kvsname");
    KVStore kvStore = this.manager.getKvStore(kvsName);
    PMIResponseBuilder responseBuilder = new PMIResponseBuilder()
      .append("cmd", "put_result");
    if (kvStore != null) {
      String key = kvs.get("key");
      String value = kvs.get("value");
      // Todo "no_room_in_kvs_"
      if (kvStore.containsKey(key)) {
        responseBuilder.append("rc", "-1")
          .append("msg", "duplicate_key " + key);
      } else {
        kvStore.put(key, value);
        responseBuilder.append("rc", "0")
          .append("msg", "success");
      }
    } else {
      responseBuilder.append("rc", "-1")
        .append("msg", "kvs_" + kvsName + "_not_found");
    }
    ctx.write(responseBuilder.build());
  }

  private void onGet(Map<String, String> kvs, ChannelHandlerContext ctx) {
    String kvsName = kvs.get("kvsname");
    KVStore kvStore = this.manager.getKvStore(kvsName);
    PMIResponseBuilder responseBuilder = new PMIResponseBuilder()
      .append("cmd", "get_result");
    String rc;
    String msg;
    String value;
    if (kvStore != null) {
      String key = kvs.get("key");
      value = kvStore.get(key);
      if (value != null) {
        rc = "0";
        msg = "success";
      } else {
        rc = "-1";
        value = "unknown";
        msg = "key_" + key + "_not_found";
      }
    } else {
      rc = "-1";
      value = "unknown";
      msg = "kvs_" + kvsName + "_not_found";
    }
    String result = responseBuilder.append("rc", rc)
      .append("msg", msg)
      .append("value", value).build();
    ctx.write(result);
  }

  private void onAbort(Map<String, String> kvs, ChannelHandlerContext ctx) {

  }

  private void onGetUniverseSize(Map<String, String> kvs, ChannelHandlerContext ctx){
    PMIResponseBuilder builder = new PMIResponseBuilder()
      .append("cmd", "universe_size")
      .append("size", this.manager.getUniverseSize());
    ctx.write(builder.build());
  }

  private void onCreateKVS(Map<String, String> kvs, ChannelHandlerContext ctx) {

  }

  private void onDestoryKVS(Map<String, String> kvs, ChannelHandlerContext ctx) {

  }
}