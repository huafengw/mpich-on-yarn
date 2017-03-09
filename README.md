# mpich-on-yarn [![Build Status](https://travis-ci.org/huafengw/mpich-on-yarn.svg?branch=master)](https://travis-ci.org/huafengw/mpich-on-yarn) [![codecov.io](https://codecov.io/github/huafengw/mpich-on-yarn/coverage.svg?branch=master)](https://codecov.io/github/huafengw/mpich-on-yarn?branch=master)
A prototype of running mpich applications on Yarn

## How to run cpi example on a single-node Yarn cluster

1. Download this project source code to ${MPICH_YANR_HOME} and build out the jar file using command

  ```
  mvn package
  ```
  
2. Prepare a mpich executable file, cpi for example. Please refer to MPICH's offical guides for this part.

3. Submit the application using command

  ```
  ${HADOOP_HOME}/bin/yarn jar ${MPICH_YANR_HOME}/target/mpich-on-yarn-1.0-SNAPSHOT-jar-with-dependencies.jar -np 2 -exec ${CPI_PATH} -wdir /tmp
  ```
  
  Here `-np` options means launch how many containers (mpich process) and `${CPI_PATH}` must be the absolute path of cpi executable file.
  
4. Now the mpich related logs are all located in containers' log directory.

## How to run parent and child (example of using MPI_Comm_spawn) in Mpich

1. Compile the child.c to an executable file

  ```
  mpicxx ${MPICH_SOURCE_DIR}/examples/child.c -o child
  ```
  
2. Compile the parent.c to an executable file

  Before compiling parent.c we need to change the source code of it. Change the child executable file path to its absolute path in function call `MPI_Comm_spawn` like
  
  ```
  err = MPI_Comm_spawn("${MPICH_SOURCE_DIR}/examples/child", MPI_ARGV_NULL, 4, ...
  ```
  
  Then compile the parent.c to an executable file
  
  ```
  mpicxx ${MPICH_SOURCE_DIR}/examples/parent.c -o parent
  ```

3. Submit the application using command

  ```
  ${HADOOP_HOME}/bin/yarn jar ${MPICH_YANR_HOME}/target/mpich-on-yarn-1.0-SNAPSHOT-jar-with-dependencies.jar -np 4 -exec ${PARENT_PATH} -wdir /tmp
  ```
  
  Here `-np` must be 4 and `${PARENT_PATH}` must be the absolute path of parent executable file. And also please note this example will launch 4 more child process, which means it will allocate 4 more containers from Yarn. So this example will launch 9 containers in total, 1 for AppMaster, 4 for parent and 4 for child. Please make sure the Yarn cluster has at least 9216 MB memory because the default memory size of each container is 1024 MB.

## Todo

1. Redirect container's log to client side.

2. Refactor client side IO server to Netty.

3. Upload the mpich executable to HDFS and make it as container's local resource.
 

