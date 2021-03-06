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
package org.apache.hadoop.mpich.util;

import org.junit.Assert;

public class TestKVStore {

  @org.junit.Test
  public void testKVStore() {
    KVStore example = new KVStore("ex1");
    example.put(new KVPair("kv", "pair"));
    example.put("3", "5");
    example.put("hwz", "518");
    example.put("\n", "sh");
    Assert.assertEquals("ex1", example.getName());
    Assert.assertEquals(true, example.containsKey("kv"));
    Assert.assertEquals("pair", example.get("kv"));
    Assert.assertEquals(new KVPair("\n", "sh"), example.getKVPairByIdx(3));
  }
}
