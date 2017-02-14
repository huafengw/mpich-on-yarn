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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class KVStore {
  private String name;
  private Map<String, String> store;

  public KVStore(String name) {
    this.name = name;
    this.store = new LinkedHashMap<String, String>();
  }

  public Boolean containsKey(String key) {
    return this.store.containsKey(key);
  }

  public synchronized void put(String key, String value) {
    this.store.put(key, value);
  }

  public String get(String key) {
    return this.store.get(key);
  }

  public KVPair getKVPairByIdx(int index) {
    if (index > store.size() || index < 0) {
      return null;
    } else {
      String key = new ArrayList<String>(store.keySet()).get(index);
      return new KVPair(key, store.get(key));
    }
  }

  public String getName() {
    return name;
  }
}
