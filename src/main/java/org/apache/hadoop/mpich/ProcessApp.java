package org.apache.hadoop.mpich;

import org.apache.hadoop.mpich.util.Constants;

import java.util.ArrayList;
import java.util.List;

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
  private List<String> args;

  // Number of processes in this app
  private int numProcess;

  // Architecture type
  private String arch;

  // Search path for executables
  private String path;

  // Working directory
  private String wdir;

  public ProcessApp() {
    this.args = new ArrayList<String>();
    this.hostName = Constants.ANY_HOST;
  }

  public String getArch() {
    return arch;
  }

  public void setArch(String arch) {
    this.arch = arch;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getWdir() {
    return wdir;
  }

  public void setWdir(String wdir) {
    this.wdir = wdir;
  }

  public void addArg(int index, String arg) {
    this.args.add(index, arg);
  }

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

  public List<String> getArgs() {
    return args;
  }

  public void setArgs(List<String> args) {
    this.args = args;
  }

  public int getNumProcess() {
    return numProcess;
  }

  public void setNumProcess(int numProcess) {
    this.numProcess = numProcess;
  }
}
