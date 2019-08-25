该工程为H3C DataEngine产品中MapReduce组件提供样例代码，而且区分kerberos环境和非kerberos环境。该工程的目录结构介绍：

MapReduceExampleCode/
  |Readme.txt                      --介绍文档
  |mapreduce-example-normal/       --非Kerberos环境下样例代码
     |pom.xml                      --pom文件
     |src/	                   --mapreduce词频统计样例代码（创建目录、文件等）
  |mapreduce-example-security/     --Kerberos环境下样例代码
     |pom.xml			   --pom文件
     |src/	                   --mapreduce词频统计样例代码（创建目录、文件等）

