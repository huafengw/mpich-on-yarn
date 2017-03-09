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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mpich.ProcessApp;
import org.apache.hadoop.mpich.ProcessWorld;
import org.apache.hadoop.mpich.util.KVStore;
import org.apache.hadoop.mpich.util.KVStoreFactory;

public class SpawnCommandParser {
  private static Log logger = LogFactory.getLog(SpawnCommandParser.class);
  private int preput_num = -1;
  private int totalSpawns = Integer.MAX_VALUE;
  private int spawnSoFar = 0;
  private ProcessWorld processWorld = new ProcessWorld(true);
  private ProcessApp currentApp = new ProcessApp();
  private String currentKey = null;
  private int curInfoIdx = -1;
  private String curInfoKey = null;

  public void process(String msg) {
//    logger.info("processing msg: " + msg);
    if (msg.trim().equals("endcmd")) {
      this.spawnSoFar += 1;
      this.processWorld.addProcessApp(currentApp);
      this.currentApp = new ProcessApp();
    } else {
      String[] kv = msg.split("=");
      if (kv.length == 2) {
        String key = kv[0];
        String value = kv[1];
        if (key.equals("nprocs")) {
          currentApp.setNumProcess(Integer.valueOf(value));
        } else if (key.equals("execname")) {
          currentApp.setExeName(value);
        } else if (key.equals("totspawns")) {
          this.totalSpawns = Integer.valueOf(value);
        } else if (key.equals("spawnssofar")) {
          //this.spawnSoFar = Integer.valueOf(value);
          currentApp.setAppNum(this.spawnSoFar - 1);
        } else if (key.equals("argcnt")) {
          currentApp.setArgNum(Integer.valueOf(value));
        } else if (key.startsWith("arg")) {
          int index = Integer.valueOf(key.substring(3));
          currentApp.addArg(index, value);
        } else if (key.equals("preput_num")) {
          this.preput_num = Integer.valueOf(value);
        } else if (key.startsWith("preput_key_")) {
          this.currentKey = value;
        } else if (key.startsWith("preput_val_")) {
          processWorld.getKvStore().put(this.currentKey, value);
        } else if (key.equals("info_num")) {
          //
        } else if (key.startsWith("info_key_")) {
          this.curInfoIdx = Integer.valueOf(key.substring(9));
          this.curInfoKey = value;
        } else if (key.startsWith("info_val_")) {
          int idx = Integer.valueOf(key.substring(9));
          if (idx != this.curInfoIdx) {
            //throw new Exception("");
          } else {
            this.putInfoKey(this.currentApp, this.curInfoKey, value);
          }
        } else {
          // throw
        }
      } else {
        // Todo
      }
    }
  }

  private void putInfoKey(ProcessApp app, String key, String value) {
    if (key.equals("host")) {
      app.setHostName(value);
    } else if (key.equals("arch")) {
      app.setArch(value);
    } else if (key.equals("wdir")) {
      app.setWdir(value);
    } else if (key.equals("path")) {
      app.setPath(value);
    } else if (key.equals("soft")) {
      // Todo
    } else {
      // throw
    }
  }

  public boolean allMsgProcessed() {
    return this.totalSpawns == this.spawnSoFar;
  }

  public ProcessWorld getProcessWorld() {
    return this.processWorld;
  }
}
