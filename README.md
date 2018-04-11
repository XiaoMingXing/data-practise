### Installation guide

#### Hadoop Cluster
https://linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/

#### Spark 
https://linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/

#### YARN Config default properties
https://hadoop.apache.org/docs/r2.4.1/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

- Show yarn logs
`yarn logs -applicationId <Application ID>`

- Spark submit job test
`spark-submit --deploy-mode client  --class org.apache.spark.examples.SparkPi  $SPARK_HOME/examples/jars/spark-examples_2.11-2.2.0.jar 10`
`spark-submit --class com.spark.secondarysort.SecondarySort     --master yarn     --deploy-mode client     ~/jars/data-algorithms-1.0-SNAPSHOT-jar-with-dependencies.jar    sampleinput.txt spark_output`

- MapReduce job test
`hadoop jar ~/jars/data-algorithms-1.0-SNAPSHOT-jar-with-dependencies.jar com.mapreduce2.secondarysort.SecondarySortDriver sampleinput.txt mr_output`
`yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.0.1.jar com.mapreduce2.secondarysort.SecondarySortDriver`

- copy the folder
`scp yarn-site.xml node2:~/hadoop/etc/hadoop`

###ISSUES

####Container exited with a non-zero exit code 127
Solution: sudo ln -s /usr/bin/java /bin/java

####Job 0 cancelled because SparkContext was shut down

####Failed to read the attempts of the application application_1523372802344_0004.

####file:/home/ubuntu/~/hadoop/tmp/nm-local-dir/usercache/ubuntu/appcache/application_1523447807755_0004/~/hadoop/tmp/nm-local-dir/usercache/ubuntu does not exist
The container logs should be under yarn.nodemanager.log-dirs:


###TODO

