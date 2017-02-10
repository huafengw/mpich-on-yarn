package org.apache.hadoop.mpich.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class IOMessageHandler {
  private int nproc;
  private int portNum;
  private ServerSocket serverSocket;
  private IOMessagesThread[] ioThreads;

  public IOMessageHandler(int nproc) throws IOException {
    this.nproc = nproc;
    // np = number of processes , + 1 for Application Master container
    this.ioThreads = new IOMessagesThread[nproc + 1];
    this.serverSocket = new ServerSocket(0);
    this.portNum = serverSocket.getLocalPort();
  }

  public void acceptAll() throws IOException {
    for (int i = 0; i < (nproc + 1); i++) {
      try {
        Socket sock = serverSocket.accept();
        //acceptAll IO thread to read STDOUT and STDERR from wrappers
        IOMessagesThread io = new IOMessagesThread(sock);
        ioThreads[i] = io;
        ioThreads[i].start();
      } catch (Exception e) {
        System.err.println("Error accepting connection from peer serverSocket..");
        e.printStackTrace();
      }
    }
  }

  public void join() throws InterruptedException {
    for (int i = 0; i < (nproc + 1); i++) {
      this.ioThreads[i].join();
    }
  }

  public int getPortNum() {
    return this.portNum;
  }
}
