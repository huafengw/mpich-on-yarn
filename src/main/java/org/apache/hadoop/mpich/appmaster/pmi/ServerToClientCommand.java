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

public enum ServerToClientCommand {
  SINGINIT_INFO("singinit_info"),
  SINGLINIT("singinit"),
  GETBYIDX_RESULTS("getbyidx_results"),
  MAXES("maxes"),
  RESPONSE_TO_INIT("response_to_init"),
  APPNUM("appnum"),
  UNIVERSE_SIZE("universe_size"),
  MY_KVSNAME("my_kvsname"),
  GET_RESULT("get_result"),
  PUT_RESULT("put_result"),
  KVS_DESTROYED("kvs_destroyed"),
  NEWKVS("newkvs"),
  BARRIER_OUT("barrier_out"),
  FINALIZE_ACK("finalize_ack");


  private final String text;

  private ServerToClientCommand(final String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return text;
  }
}
