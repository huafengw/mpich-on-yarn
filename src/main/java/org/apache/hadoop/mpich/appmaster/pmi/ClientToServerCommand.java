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

public enum ClientToServerCommand {
  BARRIER_IN("barrier_in"),
  FINALIZE("finalize"),
  ABORT("abort"),
  CREATE_KVS("create_kvs"),
  DESTORY_KVS("destroy_kvs"),
  PUT("put"),
  GET("get"),
  GET_MY_KVSNAME("get_my_kvsname"),
  INIT("init"),
  GET_MAXES("get_maxes"),
  GETBYIDX("getbyidx"),
  INITACK("initack"),
  SPAWN("spawn"),
  GET_UNIVERSE_SIZE("get_universe_size"),
  GET_APPNUM("get_appnum"),
  UNRECOGNIZED("unrecognized");

  private final String text;

  private ClientToServerCommand(final String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return text;
  }
}
