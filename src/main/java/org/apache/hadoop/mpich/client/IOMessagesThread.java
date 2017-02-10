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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class IOMessagesThread extends Thread {

  Socket clientSock;

  public IOMessagesThread(Socket clientSock) {
    this.clientSock = clientSock;
  }

  @Override
  public void run() {
    serverSocketInit();
  }

  private void serverSocketInit() {
    Scanner input = null;
    PrintWriter output = null;
    try {
      input = new Scanner(clientSock.getInputStream());
      output = new PrintWriter(clientSock.getOutputStream(), true);
      String message = input.nextLine();
      while (!(message.endsWith("EXIT"))) {
        if (!message.startsWith("@Ping#"))
          System.out.println(message);
        message = input.nextLine();
      }

    } catch (Exception cce) {
      cce.printStackTrace();
    } finally {
      try {
        clientSock.close();
        input.close();
        output.close();
      } catch (IOException ioEx) {
        ioEx.printStackTrace();
      }
    }
  }

}
