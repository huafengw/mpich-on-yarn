package org.apache.hadoop.mpich.appmaster;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

public class MpiProcessManager {
  private Map<Integer, Channel> channels = new HashMap<Integer, Channel>();
  private Map<Integer, MpiProcess> processes = new HashMap<Integer, MpiProcess>();
  private int numProcesses;
  private int nInBarrier;

  public int getNumProcesses() {
    return this.numProcesses;
  }

  public MpiProcess getProcess(int pmiid) {
    return this.processes.get(pmiid);
  }

  public synchronized void addClient(int pmiid, Channel channel) {
    channels.put(pmiid, channel);
  }


}
