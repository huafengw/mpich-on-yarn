package org.apache.hadoop.mpich;

public class ProcessApp {
  // Appnum of this group
  private int appNum;

  // Executable to run
  private String exeName;

  // Default host (can be overridded by each process in an App)
  private String hostName;

  // Number of args
  private int argNum;

  // Array of args
  private String args;

  // Number of processes in this app
  private int numProcess;

  public int getAppNum() {
    return appNum;
  }

  public void setAppNum(int appNum) {
    this.appNum = appNum;
  }

  public String getExeName() {
    return exeName;
  }

  public void setExeName(String exeName) {
    this.exeName = exeName;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getArgNum() {
    return argNum;
  }

  public void setArgNum(int argNum) {
    this.argNum = argNum;
  }

  public String getArgs() {
    return args;
  }

  public void setArgs(String args) {
    this.args = args;
  }

  public int getNumProcess() {
    return numProcess;
  }

  public void setNumProcess(int numProcess) {
    this.numProcess = numProcess;
  }
}
