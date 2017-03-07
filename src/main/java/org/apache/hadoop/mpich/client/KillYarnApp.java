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
package org.apache.hadoop.mpich.client;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;

public class KillYarnApp extends Thread {

  private ApplicationId appId;
  private YarnClient appManager;

  public KillYarnApp(ApplicationId appId, YarnClient appManager) {
    this.appId = appId;
    this.appManager = appManager;
  }

  @Override
  public void run() {
    if (Client.isRunning) {
      try {
          System.out.println("Killing Application Forcefully");
          appManager.killApplication(appId); 
      } catch (Exception exp) {
        System.err.println("Error when Killing Application");
        exp.printStackTrace();
      }
    }
  }
}
