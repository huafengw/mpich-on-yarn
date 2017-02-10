package org.apache.hadoop.mpich.appmaster;

import org.apache.hadoop.mpich.appmaster.pmi.ClientToServerCommand;
import org.apache.hadoop.mpich.util.Utils;

import java.util.Map;

public class Test {
  public static void main(String[] args) throws Exception {
    ClientToServerCommand command = ClientToServerCommand.valueOf("init".toUpperCase());
    System.out.println(command);

    String msg = "key1=value2  key2=value3 key3=value4";
    Map<String, String> result = Utils.parseKeyVals(msg);
    System.out.println(result);
  }
}
