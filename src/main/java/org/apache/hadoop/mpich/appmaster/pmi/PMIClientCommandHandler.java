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

import io.netty.channel.Channel;
import org.apache.hadoop.mpich.MpiProcess;
import org.apache.hadoop.mpich.MpiProcessGroup;
import org.apache.hadoop.mpich.appmaster.MpiProcessManager;
import org.apache.hadoop.mpich.util.KVPair;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.PMIResponseBuilder;
import org.apache.hadoop.mpich.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PMIClientCommandHandler {
  private static List<String> EMPTY_RESPONSE = new ArrayList<String>();
  private Channel channel;
  private MpiProcessManager manager;
  private boolean inSpawn = false;
  private SpawnCommandHandler spawnCommandHandler = new SpawnCommandHandler();

  public PMIClientCommandHandler(MpiProcessManager manager, Channel channel) {
    this.manager = manager;
    this.channel = channel;
  }

  public List<String> process(String msg) throws Exception {
    if (inSpawn) {
      return this.processSpawn(msg);
    } else {
      return this.processNormalComm(msg);
    }
  }

  private List<String> processSpawn(String msg) {
    this.spawnCommandHandler.process(msg);
    if (this.spawnCommandHandler.allMsgProcessed()) {
      List<String> response = new ArrayList<String>();

      response.add(new PMIResponseBuilder()
        .append("cmd", "spawn_result")
        .append("rc", 0).build());
      this.spawnCommandHandler = new SpawnCommandHandler();
      this.inSpawn = false;
      return response;
    } else {
      return EMPTY_RESPONSE;
    }
  }

  private List<String> processNormalComm(String msg) throws Exception {
    Map<String, String> kvs = Utils.parseKeyVals(msg);
    ClientToServerCommand command = Utils.getCommand(kvs);
    List<String> responses = new ArrayList<String>();
    switch (command) {
      case SPAWN:
        this.inSpawn = true;
        break;
      case INITACK:
        responses = this.onInitAck(kvs);
        break;
      case INIT:
        responses = this.onInit(kvs);
        break;
      case GET_MAXES:
        responses =  this.onGetMaxes(kvs);
        break;
      case GET_APPNUM:
        responses = this.onGetAppNum(kvs);
        break;
      case CREATE_KVS:
        responses = this.onCreateKVS(kvs);
        break;
      case DESTORY_KVS:
        responses = this.onDestoryKVS(kvs);
        break;
      case GET_MY_KVSNAME:
        responses = this.onGetMyKvsName(kvs);
        break;
      case GETBYIDX:
        responses = this.onGetByIdx(kvs);
        break;
      case PUT:
        responses = this.onPut(kvs);
        break;
      case GET:
        responses = this.onGet(kvs);
        break;
      case BARRIER_IN:
        responses = this.onBarrierIn(kvs);
        break;
      case ABORT:
        responses = this.onAbort(kvs);
        break;
      case GET_UNIVERSE_SIZE:
        responses = this.onGetUniverseSize(kvs);
        break;
      case FINALIZE:
        responses = this.onFinalize(kvs);
        break;
    }
    return responses;
  }

  private List<String> onInitAck(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    Integer pmiid = Integer.parseInt(kvs.get("pmiid"));
    MpiProcess process = this.manager.getProcessById(pmiid);
    manager.addClient(pmiid, this.channel);
    response.add(new PMIResponseBuilder().append("cmd", "initack").build());
    response.add(new PMIResponseBuilder().append("cmd", "set")
      .append("size", String.valueOf(process.getGroup().getNumProcesses())).build());
    response.add(new PMIResponseBuilder().append("cmd", "set")
      .append("rank", String.valueOf(process.getRank())).build());
    response.add(new PMIResponseBuilder().append("cmd", "set")
      .append("debug", "1").build());
    return response;
  }

  private List<String> onInit(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    String pmi_version = kvs.get("pmi_version");
    String pmi_subversion = kvs.get("pmi_subversion");
    String msg = new PMIResponseBuilder()
      .append("cmd", "response_to_init")
      .append("pmi_version", pmi_version)
      .append("pmi_subversion", pmi_subversion)
      .append("rc", "0").build();
    response.add(msg);
    return response;
  }

  private List<String> onGetMaxes(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    String msg = new PMIResponseBuilder()
      .append("cmd", "maxes")
      .append("kvsname_max", "256")
      .append("keylen_max", "64")
      .append("vallen_max", "256").build();
    response.add(msg);
    return response;
  }

  private List<String> onGetAppNum(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    response.add(new PMIResponseBuilder().
      append("cmd", "appnum").append("appnum", "0").build());
    return response;
  }

  private List<String> onGetMyKvsName(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    MpiProcess process = this.manager.getProcessByChannel(this.channel);
    if (process != null) {
      String storeName = process.getGroup().getKvStore().getName();
      response.add(new PMIResponseBuilder().
        append("cmd", "my_kvsname").append("kvsname", storeName).build());
    }
    return response;
  }

  private List<String> onGetByIdx(Map<String, String> kvs) {
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
    List<String> response = new ArrayList<String>();
    response.add(responseBuilder.build());
    return response;
  }

  private List<String> onBarrierIn(Map<String, String> kvs) {
    MpiProcess process = this.manager.getProcessByChannel(this.channel);
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
    return EMPTY_RESPONSE;
  }

  private List<String> onFinalize(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    response.add(new PMIResponseBuilder()
      .append("cmd", "finalize_ack").build());
    return response;
  }

  private List<String> onPut(Map<String, String> kvs) {
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
          .append("msg", "duplicate_key_" + key);
      } else {
        kvStore.put(key, value);
        responseBuilder.append("rc", "0")
          .append("msg", "success");
      }
    } else {
      responseBuilder.append("rc", "-1")
        .append("msg", "kvs_" + kvsName + "_not_found");
    }
    List<String> response = new ArrayList<String>();
    response.add(responseBuilder.build());
    return response;
  }

  private List<String> onGet(Map<String, String> kvs) {
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
    List<String> response = new ArrayList<String>();
    response.add(result);
    return response;
  }

  private List<String> onGetUniverseSize(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    response.add(new PMIResponseBuilder()
      .append("cmd", "universe_size")
      .append("size", this.manager.getUniverseSize()).build());
    return response;
  }

  private List<String> onAbort(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    return response;
  }

  private List<String> onCreateKVS(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    return response;
  }

  private List<String> onDestoryKVS(Map<String, String> kvs) {
    List<String> response = new ArrayList<String>();
    return response;
  }
}
