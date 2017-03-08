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

## Todo

1. Redirect container's log to client side.

2. Shutdown the yarn application properly.

3. Upload the mpich executable to HDFS and make it as container's local resource.
 

