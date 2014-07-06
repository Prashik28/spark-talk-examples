cd ~/spark-talk/spark-jobserver/target/scala-2.10
curl --data-binary @spark-jobserver-example_2.10-0.1-SNAPSHOT.jar localhost:8090/jars/test

cd ~/spark-talk/examples
curl --data-binary @example.conf 'localhost:8090/jobs?appName=test&classPath=example.SparkExample'